// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/wrgl/core/pkg/api"
)

type ReceivePackSessionStore interface {
	Set(sid uuid.UUID, ses *ReceivePackSession)
	Get(sid uuid.UUID) (ses *ReceivePackSession, ok bool)
	Delete(sid uuid.UUID)
}

func (s *Server) getReceivePackSession(r *http.Request, sessions ReceivePackSessionStore) (ses *ReceivePackSession, sid uuid.UUID, err error) {
	var ok bool
	c, err := r.Cookie(api.CookieReceivePackSession)
	if err == nil {
		sid, err = uuid.Parse(c.Value)
		if err != nil {
			return
		}
		ses, ok = sessions.Get(sid)
		if !ok {
			ses = nil
		}
	}
	if ses == nil {
		sid, err = uuid.NewRandom()
		if err != nil {
			return
		}
		repo := getRepo(r)
		db := s.getDB(repo)
		rs := s.getRS(repo)
		c := s.getConf(repo)
		ses = NewReceivePackSession(db, rs, c, sid)
		sessions.Set(sid, ses)
	}
	return
}

func (s *Server) handleReceivePack(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "forbidden", http.StatusForbidden)
		return
	}
	repo := getRepo(r)
	sessions := s.getRPSession(repo)
	ses, sid, err := s.getReceivePackSession(r, sessions)
	if err != nil {
		panic(err)
	}
	defer func() {
		if s := recover(); s != nil {
			sessions.Delete(sid)
			panic(s)
		}
	}()
	if done := ses.ServeHTTP(rw, r); done {
		sessions.Delete(sid)
	}
}
