package apiserver

import (
	"encoding/json"
	"net/http"

	"github.com/wrgl/core/pkg/api"
)

func (s *Server) handleGetConfig(rw http.ResponseWriter, r *http.Request) {
	repo := getRepo(r)
	cs := s.getConfS(repo)
	c, err := cs.Open()
	if err != nil {
		panic(err)
	}
	b, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	rw.Header().Set("Content-Type", api.CTJSON)
	_, err = rw.Write(b)
	if err != nil {
		panic(err)
	}
}
