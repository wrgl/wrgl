// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/wrgl/core/pkg/api"
)

type UploadPackSessionStore interface {
	Set(sid uuid.UUID, ses *UploadPackSession)
	Get(sid uuid.UUID) (ses *UploadPackSession, ok bool)
	Delete(sid uuid.UUID)
}

func (s *Server) getUploadPackSession(r *http.Request, sessions UploadPackSessionStore) (ses *UploadPackSession, sid uuid.UUID, err error) {
	var ok bool
	c, err := r.Cookie(api.CookieUploadPackSession)
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
		cs := s.getConfS(repo)
		c, err := cs.Open()
		if err != nil {
			panic(err)
		}
		ses = NewUploadPackSession(db, rs, sid, c.MaxPackFileSize())
		sessions.Set(sid, ses)
	}
	return
}

func (s *Server) handleUploadPack(rw http.ResponseWriter, r *http.Request) {
	repo := getRepo(r)
	sessions := s.getUpSession(repo)
	ses, sid, err := s.getUploadPackSession(r, sessions)
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
