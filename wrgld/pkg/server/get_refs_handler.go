package server

import (
	"net/http"

	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/ref"
)

func (s *Server) handleGetRefs(rw http.ResponseWriter, r *http.Request) {
	rs := s.getRS(r)
	refs, err := ref.ListLocalRefs(rs)
	if err != nil {
		panic(err)
	}
	resp := &payload.GetRefsResponse{
		Refs: map[string]*payload.Hex{},
	}
	for k, v := range refs {
		h := &payload.Hex{}
		copy((*h)[:], v)
		resp.Refs[k] = h
	}
	WriteJSON(rw, resp)
}
