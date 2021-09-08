package apiserver

import (
	"net/http"
)

func (s *Server) handleGetConfig(rw http.ResponseWriter, r *http.Request) {
	cs := s.getConfS(r)
	c, err := cs.Open()
	if err != nil {
		panic(err)
	}
	writeJSON(rw, c)
}
