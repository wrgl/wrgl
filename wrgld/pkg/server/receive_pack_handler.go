package server

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/wrgl/wrgl/pkg/api"
	apiutils "github.com/wrgl/wrgl/pkg/api/utils"
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
		db := s.getDB(r)
		rs := s.getRS(r)
		cs := s.getConfS(r)
		c, err := cs.Open()
		if err != nil {
			panic(err)
		}
		opts := make([]apiutils.ObjectReceiveOption, len(s.receiverOpts))
		copy(opts, s.receiverOpts)
		if s.debugOut != nil {
			opts = append(opts, apiutils.WithReceiverDebugOut(s.debugOut))
		}
		ses = NewReceivePackSession(db, rs, c, sid, opts...)
		sessions.Set(sid, ses)
	}
	return
}

func (s *Server) handleReceivePack(rw http.ResponseWriter, r *http.Request) {
	sessions := s.getRPSession(r)
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
