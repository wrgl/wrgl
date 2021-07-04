// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package pack

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/objects"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/ref"
)

var zeroOID = strings.Repeat("0", 32)

type ReceivePackHandler struct {
	db objects.Store
	rs ref.Store
	c  *conf.Config
}

func NewReceivePackHandler(db objects.Store, rs ref.Store, c *conf.Config) *ReceivePackHandler {
	return &ReceivePackHandler{
		db: db,
		rs: rs,
		c:  c,
	}
}

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

func (h *ReceivePackHandler) saveObjects(pr *encoding.PackfileReader) error {
	if pr == nil {
		return nil
	}
	for {
		ot, oc, err := pr.ReadObject()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch ot {
		case encoding.ObjectCommit:
			_, err = objects.SaveCommit(h.db, oc)
		case encoding.ObjectTable:
			_, err = objects.SaveTable(h.db, oc)
		case encoding.ObjectBlock:
			_, err = objects.SaveBlock(h.db, oc)
		default:
			panic(fmt.Sprintf("unrecognized object type %v", ot))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *ReceivePackHandler) saveRefs(updates []*packutils.Update) error {
	for _, u := range updates {
		if u.Sum == nil {
			if *h.c.Receive.DenyDeletes {
				u.ErrMsg = "remote does not support deleting refs"
				continue
			} else {
				err := ref.DeleteRef(h.rs, strings.TrimPrefix(u.Dst, "refs/"))
				if err != nil {
					return err
				}
			}
		} else if !objects.CommitExist(h.db, u.Sum) {
			u.ErrMsg = "remote did not receive commit"
			continue
		}
		oldSum, _ := ref.GetRef(h.rs, strings.TrimPrefix(u.Dst, "refs/"))
		var msg string
		if oldSum != nil {
			if string(u.OldSum) != string(oldSum) {
				u.ErrMsg = "remote ref updated since checkout"
				continue
			} else if *h.c.Receive.DenyNonFastForwards {
				fastForward, err := ref.IsAncestorOf(h.db, oldSum, u.Sum)
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
		err := ref.SaveRef(h.rs, strings.TrimPrefix(u.Dst, "refs/"), u.Sum, h.c.User.Name, h.c.User.Email, "receive-pack", msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *ReceivePackHandler) reportStatus(rw http.ResponseWriter, updates []*packutils.Update) error {
	rw.Header().Set("Content-Type", "application/x-wrgl-receive-pack-result")
	buf := misc.NewBuffer(nil)
	err := encoding.WritePktLine(rw, buf, "unpack ok")
	if err != nil {
		return err
	}
	for _, u := range updates {
		if u.ErrMsg == "" {
			err = encoding.WritePktLine(rw, buf, fmt.Sprintf("ok %s", u.Dst))
			if err != nil {
				return err
			}
		} else {
			err = encoding.WritePktLine(rw, buf, fmt.Sprintf("ng %s %s", u.Dst, u.ErrMsg))
			if err != nil {
				return err
			}
		}
	}
	return encoding.WritePktLine(rw, buf, "")
}

func (h *ReceivePackHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "forbidden", http.StatusForbidden)
		return
	}
	updates, pr, err := parseReceivePackRequest(r.Body)
	if err != nil {
		panic(err)
	}
	err = h.saveObjects(pr)
	if err != nil {
		panic(err)
	}
	err = h.saveRefs(updates)
	if err != nil {
		panic(err)
	}
	err = h.reportStatus(rw, updates)
	if err != nil {
		panic(err)
	}
}
