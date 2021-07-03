// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package pack

import (
	"compress/gzip"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/objects"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/ref"
)

type UploadPackHandler struct {
	db          objects.Store
	rs          ref.Store
	negotiators map[string]*Negotiator
	Path        string
}

func NewUploadPackHandler(db objects.Store, rs ref.Store) *UploadPackHandler {
	return &UploadPackHandler{
		db:          db,
		rs:          rs,
		Path:        "/upload-pack/",
		negotiators: map[string]*Negotiator{},
	}
}

func (h *UploadPackHandler) getNegotiation(r *http.Request) (neg *Negotiator, nid string, err error) {
	var ok bool
	c, err := r.Cookie("negotiation-id")
	if err == nil {
		nid = c.Value
		neg, ok = h.negotiators[nid]
		if !ok {
			neg = nil
		}
	}
	if neg == nil {
		neg = NewNegotiator()
		var id uuid.UUID
		id, err = uuid.NewRandom()
		if err != nil {
			return
		}
		nid = id.String()
	}
	return
}

func (h *UploadPackHandler) sendACKs(rw http.ResponseWriter, nid string, neg *Negotiator, acks [][]byte) {
	rw.Header().Set("Content-Type", "application/x-wrgl-upload-pack-result")
	http.SetCookie(rw, &http.Cookie{
		Name:     "negotiation-id",
		Value:    nid,
		Path:     h.Path,
		HttpOnly: true,
		MaxAge:   3600 * 24,
	})
	h.negotiators[nid] = neg
	buf := misc.NewBuffer(nil)
	for _, ack := range acks {
		err := encoding.WritePktLine(rw, buf, "ACK "+hex.EncodeToString(ack))
		if err != nil {
			panic(err)
		}
	}
	err := encoding.WritePktLine(rw, buf, "NAK")
	if err != nil {
		panic(err)
	}
}

func (h *UploadPackHandler) sendPackfile(rw http.ResponseWriter, r *http.Request, neg *Negotiator) {
	rw.Header().Set("Content-Type", "application/x-wrgl-packfile")
	rw.Header().Set("Content-Encoding", "gzip")

	c, err := r.Cookie("negotiation-id")
	if err == nil {
		http.SetCookie(rw, &http.Cookie{
			Name:     "negotiation-id",
			Value:    c.Value,
			Path:     h.Path,
			HttpOnly: true,
			Expires:  time.Time{},
		})
		delete(h.negotiators, c.Value)
	}

	gzw := gzip.NewWriter(rw)
	defer gzw.Close()

	err = packutils.WriteCommitsToPackfile(h.db, neg.CommitsToSend(), neg.CommonCommmits(), gzw)
	if err != nil {
		panic(err)
	}
}

func parseUploadPackRequest(r io.Reader) (wants, haves [][]byte, done bool, err error) {
	parser := encoding.NewParser(r)
	var s string
reading:
	for {
		s, err = encoding.ReadPktLine(parser)
		if err != nil {
			return
		}
		if s == "" {
			break
		}
		fields := strings.Fields(s)
		switch fields[0] {
		case "done":
			done = true
			break reading
		case "want":
			b := make([]byte, 16)
			_, err = hex.Decode(b, []byte(fields[1]))
			if err != nil {
				return
			}
			wants = append(wants, b)
		case "have":
			b := make([]byte, 16)
			_, err = hex.Decode(b, []byte(fields[1]))
			if err != nil {
				return
			}
			haves = append(haves, b)
		default:
			err = NewBadRequestError("unrecognized command %q", fields[0])
			return
		}
	}
	return
}

func (h *UploadPackHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "forbidden", http.StatusForbidden)
		return
	}
	wants, haves, done, err := parseUploadPackRequest(r.Body)
	if err != nil {
		panic(err)
	}
	neg, nid, err := h.getNegotiation(r)
	if err != nil {
		panic(err)
	}
	acks, err := neg.HandleUploadPackRequest(h.db, h.rs, wants, haves, done)
	if err != nil {
		if v, ok := err.(*BadRequestError); ok {
			http.Error(rw, v.Message, http.StatusBadRequest)
			return
		}
		panic(err)
	}
	if len(acks) > 0 {
		h.sendACKs(rw, nid, neg, acks)
	} else {
		h.sendPackfile(rw, r, neg)
	}
}
