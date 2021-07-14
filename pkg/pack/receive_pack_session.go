// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package pack

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/objects"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/ref"
)

const (
	receivePackSessionCookie = "receive-pack-session-id"
	CTReceivePackResult      = "application/x-wrgl-receive-pack-result"
)

func parseReceivePackRequest(r io.Reader) (updates []*packutils.Update, pr *encoding.PackfileReader, err error) {
	parser := encoding.NewParser(r)
	var s string
	readPack := false
	for {
		s, err = encoding.ReadPktLine(parser)
		if err != nil {
			return
		}
		if s == "" {
			break
		}
		var oldSum, sum []byte
		if s[:32] != zeroOID {
			oldSum = make([]byte, 16)
			_, err = hex.Decode(oldSum, []byte(s[:32]))
			if err != nil {
				return
			}
		}
		if s[33:65] != zeroOID {
			sum = make([]byte, 16)
			_, err = hex.Decode(sum, []byte(s[33:65]))
			if err != nil {
				return
			}
		}
		if sum != nil {
			readPack = true
		}
		updates = append(updates, &packutils.Update{
			Dst:    s[66:],
			OldSum: oldSum,
			Sum:    sum,
		})
	}
	if readPack {
		pr, err = encoding.NewPackfileReader(io.NopCloser(r))
		if err != nil {
			return
		}
	}
	return
}

type ReceivePackSession struct {
	db       objects.Store
	rs       ref.Store
	c        *conf.Config
	updates  []*packutils.Update
	state    stateFn
	receiver *packutils.ObjectReceiver
	path, id string
}

func NewReceivePackSession(db objects.Store, rs ref.Store, c *conf.Config, path, id string) *ReceivePackSession {
	s := &ReceivePackSession{
		db:   db,
		rs:   rs,
		c:    c,
		path: path,
		id:   id,
	}
	s.state = s.greet
	return s
}

func (s *ReceivePackSession) saveRefs() error {
	for _, u := range s.updates {
		if u.Sum == nil {
			if *s.c.Receive.DenyDeletes {
				u.ErrMsg = "remote does not support deleting refs"
				continue
			} else {
				err := ref.DeleteRef(s.rs, strings.TrimPrefix(u.Dst, "refs/"))
				if err != nil {
					return err
				}
			}
		} else if !objects.CommitExist(s.db, u.Sum) {
			u.ErrMsg = "remote did not receive commit"
			continue
		}
		oldSum, _ := ref.GetRef(s.rs, strings.TrimPrefix(u.Dst, "refs/"))
		var msg string
		if oldSum != nil {
			if string(u.OldSum) != string(oldSum) {
				u.ErrMsg = "remote ref updated since checkout"
				continue
			} else if *s.c.Receive.DenyNonFastForwards {
				fastForward, err := ref.IsAncestorOf(s.db, oldSum, u.Sum)
				if err != nil {
					return err
				} else if !fastForward {
					u.ErrMsg = "remote does not support non-fast-fowards"
					continue
				}
			}
			msg = "update ref"
		} else {
			msg = "create ref"
		}
		err := ref.SaveRef(s.rs, strings.TrimPrefix(u.Dst, "refs/"), u.Sum, s.c.User.Name, s.c.User.Email, "receive-pack", msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ReceivePackSession) greet(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	var pr *encoding.PackfileReader
	var err error
	s.updates, pr, err = parseReceivePackRequest(r.Body)
	if err != nil {
		panic(err)
	}
	commits := [][]byte{}
	for _, u := range s.updates {
		if u.Sum != nil {
			commits = append(commits, u.Sum)
		}
	}
	if len(commits) > 0 {
		s.receiver = packutils.NewObjectReceiver(s.db, commits)
		done, err := s.receiver.Receive(pr)
		if err != nil {
			panic(err)
		}
		if !done {
			return s.askForMore(rw, r)
		}
	}
	return s.reportStatus(rw, r)
}

func (s *ReceivePackSession) receiveObjects(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	_, pr, err := parseReceivePackRequest(r.Body)
	if err != nil {
		panic(err)
	}
	done, err := s.receiver.Receive(pr)
	if err != nil {
		panic(err)
	}
	if !done {
		return s.askForMore(rw, r)
	}
	return s.reportStatus(rw, r)
}

func (s *ReceivePackSession) askForMore(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	rw.Header().Set("Content-Type", CTReceivePackResult)
	http.SetCookie(rw, &http.Cookie{
		Name:     receivePackSessionCookie,
		Value:    s.id,
		Path:     s.path,
		HttpOnly: true,
		MaxAge:   3600 * 24,
	})
	buf := misc.NewBuffer(nil)
	for _, line := range []string{"unpack ok", "continue"} {
		err := encoding.WritePktLine(rw, buf, line)
		if err != nil {
			panic(err)
		}
	}
	err := encoding.WritePktLine(rw, buf, "")
	if err != nil {
		panic(err)
	}
	return s.receiveObjects
}

func (s *ReceivePackSession) reportStatus(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	err := s.saveRefs()
	if err != nil {
		panic(err)
	}
	rw.Header().Set("Content-Type", CTReceivePackResult)
	// remove cookie
	http.SetCookie(rw, &http.Cookie{
		Name:     receivePackSessionCookie,
		Value:    s.id,
		Path:     s.path,
		HttpOnly: true,
		Expires:  time.Now().Add(time.Hour * -24),
	})
	buf := misc.NewBuffer(nil)
	err = encoding.WritePktLine(rw, buf, "unpack ok")
	if err != nil {
		panic(err)
	}
	for _, u := range s.updates {
		if u.ErrMsg == "" {
			err = encoding.WritePktLine(rw, buf, fmt.Sprintf("ok %s", u.Dst))
			if err != nil {
				panic(err)
			}
		} else {
			err = encoding.WritePktLine(rw, buf, fmt.Sprintf("ng %s %s", u.Dst, u.ErrMsg))
			if err != nil {
				panic(err)
			}
		}
	}
	err = encoding.WritePktLine(rw, buf, "")
	if err != nil {
		panic(err)
	}
	return nil
}

// ServeHTTP receives, saves objects and update refs, returns true when session is
// finished and can be removed.
func (s *ReceivePackSession) ServeHTTP(rw http.ResponseWriter, r *http.Request) bool {
	s.state = s.state(rw, r)
	return s.state == nil
}
