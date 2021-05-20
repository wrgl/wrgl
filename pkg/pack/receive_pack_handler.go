// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package pack

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/mmcloughlin/meow"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/objects"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
)

var zeroOID = strings.Repeat("0", 32)

type ReceivePackHandler struct {
	db kv.DB
	fs kv.FileStore
	c  *versioning.Config
}

func NewReceivePackHandler(db kv.DB, fs kv.FileStore, c *versioning.Config) *ReceivePackHandler {
	return &ReceivePackHandler{
		db: db,
		fs: fs,
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
			_, err := versioning.SaveCommitBytes(h.db, 0, oc)
			if err != nil {
				return err
			}
		case encoding.ObjectTable:
			tr, err := objects.NewTableReader(bytes.NewReader(oc))
			if err != nil {
				return err
			}
			b := table.NewBuilder(h.db, h.fs, tr.Columns, tr.PK, 0, 0)
			_, err = b.SaveTableBytes(oc, tr.RowsCount())
			if err != nil {
				return err
			}
		case encoding.ObjectRow:
			sum := meow.Checksum(0, oc)
			err := table.SaveRow(h.db, sum[:], oc)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *ReceivePackHandler) saveRefs(updates []*packutils.Update) error {
	for _, u := range updates {
		if u.Sum == nil {
			if h.c.Receive.DenyDeletes {
				u.ErrMsg = "remote does not support deleting refs"
				continue
			} else {
				err := versioning.DeleteRef(h.db, h.fs, u.Dst)
				if err != nil {
					return err
				}
			}
		} else if !versioning.CommitExist(h.db, u.Sum) {
			u.ErrMsg = "remote did not receive commit"
			continue
		}
		oldSum, _ := versioning.GetRef(h.db, u.Dst)
		var msg string
		if oldSum != nil {
			if string(u.OldSum) != string(oldSum) {
				u.ErrMsg = "remote ref updated since checkout"
				continue
			} else if h.c.Receive.DenyNonFastForwards {
				fastForward, err := versioning.IsAncestorOf(h.db, oldSum, u.Sum)
				if err != nil {
					return err
				} else if !fastForward {
					u.ErrMsg = "remote does not support non-fast-fowards"
					continue
				}
			}
			msg = "create ref"
		} else {
			msg = "update ref"
		}
		err := versioning.SaveRef(h.db, h.fs, u.Dst, u.Sum, h.c.User.Name, h.c.User.Email, "receive-pack", msg)
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
