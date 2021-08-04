// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/wrgl/core/pkg/api"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

type ReceivePackSessionStore interface {
	Set(sid uuid.UUID, ses *ReceivePackSession)
	Get(sid uuid.UUID) (ses *ReceivePackSession, ok bool)
	Delete(sid uuid.UUID)
}

type ReceivePackHandler struct {
	db       objects.Store
	rs       ref.Store
	c        *conf.Config
	sessions ReceivePackSessionStore
}

func NewReceivePackHandler(db objects.Store, rs ref.Store, c *conf.Config, sessions ReceivePackSessionStore) *ReceivePackHandler {
	return &ReceivePackHandler{
		db:       db,
		rs:       rs,
		c:        c,
		sessions: sessions,
	}
}

func (h *ReceivePackHandler) getSession(r *http.Request) (ses *ReceivePackSession, sid uuid.UUID, err error) {
	var ok bool
	c, err := r.Cookie(api.ReceivePackSessionCookie)
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
		ses = NewReceivePackSession(h.db, h.rs, h.c, sid)
		h.sessions.Set(sid, ses)
	}
	return
}

func (h *ReceivePackHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
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
