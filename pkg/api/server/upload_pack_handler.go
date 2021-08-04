// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/wrgl/core/pkg/api"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

type UploadPackSessionStore interface {
	Set(sid uuid.UUID, ses *UploadPackSession)
	Get(sid uuid.UUID) (ses *UploadPackSession, ok bool)
	Delete(sid uuid.UUID)
}

type UploadPackHandler struct {
	db              objects.Store
	rs              ref.Store
	sessions        UploadPackSessionStore
	maxPackfileSize uint64
}

func NewUploadPackHandler(db objects.Store, rs ref.Store, sessions UploadPackSessionStore, maxPackfileSize uint64) *UploadPackHandler {
	return &UploadPackHandler{
		db:              db,
		rs:              rs,
		maxPackfileSize: maxPackfileSize,
		sessions:        sessions,
	}
}

func (h *UploadPackHandler) getSession(r *http.Request) (ses *UploadPackSession, sid uuid.UUID, err error) {
	var ok bool
	c, err := r.Cookie(api.UploadPackSessionCookie)
	if err == nil {
		sid, err = uuid.Parse(c.Value)
		if err != nil {
			return
		}
		ses, ok = h.sessions.Get(sid)
		if !ok {
			ses = nil
		}
	}
	if ses == nil {
		sid, err = uuid.NewRandom()
		if err != nil {
			return
		}
		ses = NewUploadPackSession(h.db, h.rs, sid, h.maxPackfileSize)
		h.sessions.Set(sid, ses)
	}
	return
}

func (h *UploadPackHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "forbidden", http.StatusForbidden)
		return
	}
	ses, sid, err := h.getSession(r)
	if err != nil {
		panic(err)
	}
	defer func() {
		if s := recover(); s != nil {
			h.sessions.Delete(sid)
			panic(s)
		}
	}()
	if done := ses.ServeHTTP(rw, r); done {
		h.sessions.Delete(sid)
	}
}
