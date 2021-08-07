// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wrgl/core/pkg/api"
	"github.com/wrgl/core/pkg/api/payload"
	apiutils "github.com/wrgl/core/pkg/api/utils"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

type ReceivePackSession struct {
	db       objects.Store
	rs       ref.Store
	c        *conf.Config
	updates  map[string]*payload.Update
	state    stateFn
	receiver *apiutils.ObjectReceiver
	id       uuid.UUID
}

func NewReceivePackSession(db objects.Store, rs ref.Store, c *conf.Config, id uuid.UUID) *ReceivePackSession {
	s := &ReceivePackSession{
		db: db,
		rs: rs,
		c:  c,
		id: id,
	}
	s.state = s.greet
	return s
}

func (s *ReceivePackSession) saveRefs() error {
	for dst, u := range s.updates {
		oldSum, _ := ref.GetRef(s.rs, strings.TrimPrefix(dst, "refs/"))
		if (u.OldSum == nil && oldSum != nil) || (u.OldSum != nil && !bytes.Equal(oldSum, (*u.OldSum)[:])) {
			u.ErrMsg = "remote ref updated since checkout"
			continue
		}
		var sum []byte
		if u.Sum == nil {
			if *s.c.Receive.DenyDeletes {
				u.ErrMsg = "remote does not support deleting refs"
			} else {
				err := ref.DeleteRef(s.rs, strings.TrimPrefix(dst, "refs/"))
				if err != nil {
					return err
				}
			}
			continue
		} else {
			sum = (*u.Sum)[:]
			if !objects.CommitExist(s.db, sum) {
				u.ErrMsg = "remote did not receive commit"
				continue
			}
		}
		var msg string
		if oldSum != nil {
			if *s.c.Receive.DenyNonFastForwards {
				fastForward, err := ref.IsAncestorOf(s.db, oldSum, sum)
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
		err := ref.SaveRef(
			s.rs,
			strings.TrimPrefix(dst, "refs/"),
			sum,
			s.c.User.Name,
			s.c.User.Email,
			"receive-pack",
			msg,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ReceivePackSession) greet(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	var err error
	if v := r.Header.Get("Content-Type"); v != api.CTJSON {
		http.Error(rw, "updates expected", http.StatusBadRequest)
		return nil
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}
	req := &payload.ReceivePackRequest{}
	err = json.Unmarshal(b, req)
	if err != nil {
		return
	}
	s.updates = req.Updates
	commits := [][]byte{}
	outdated := false
	for dst, u := range s.updates {
		oldSum, err := ref.GetRef(s.rs, strings.TrimPrefix(dst, "refs/"))
		if err != nil {
			oldSum = nil
		}
		if (u.OldSum == nil && oldSum != nil) || (u.OldSum != nil && !bytes.Equal(oldSum, (*u.OldSum)[:])) {
			outdated = true
			u.ErrMsg = "remote ref updated since checkout"
		}
		if u.Sum != nil {
			commits = append(commits, (*u.Sum)[:])
		}
	}
	if outdated {
		return s.reportStatus(rw, r)
	}
	if len(commits) > 0 {
		s.receiver = apiutils.NewObjectReceiver(s.db, commits)
		return s.askForMore(rw, r)
	}
	err = s.saveRefs()
	if err != nil {
		panic(err)
	}
	return s.reportStatus(rw, r)
}

func (s *ReceivePackSession) receiveObjects(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	if v := r.Header.Get("Content-Type"); v != api.CTPackfile {
		http.Error(rw, "packfile expected", http.StatusBadRequest)
		return nil
	}
	pr, err := encoding.NewPackfileReader(r.Body)
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return
	}
	done, err := s.receiver.Receive(pr)
	if err != nil {
		panic(err)
	}
	if !done {
		return s.askForMore(rw, r)
	}
	err = s.saveRefs()
	if err != nil {
		panic(err)
	}
	return s.reportStatus(rw, r)
}

func (s *ReceivePackSession) askForMore(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	http.SetCookie(rw, &http.Cookie{
		Name:     api.CookieReceivePackSession,
		Value:    s.id.String(),
		Path:     api.PathReceivePack,
		HttpOnly: true,
		MaxAge:   3600 * 3,
	})
	rw.WriteHeader(http.StatusOK)
	return s.receiveObjects
}

func (s *ReceivePackSession) reportStatus(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	rw.Header().Set("Content-Type", api.CTJSON)
	// remove cookie
	http.SetCookie(rw, &http.Cookie{
		Name:     api.CookieReceivePackSession,
		Value:    s.id.String(),
		Path:     api.PathReceivePack,
		HttpOnly: true,
		Expires:  time.Now().Add(time.Hour * -24),
	})
	resp := &payload.ReceivePackResponse{
		Updates: s.updates,
	}
	b, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	_, err = rw.Write(b)
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
