package server

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/conf"
)

func (s *Server) handlePutConfig(rw http.ResponseWriter, r *http.Request) {
	if v := r.Header.Get("Content-Type"); v != api.CTJSON {
		SendError(rw, http.StatusUnsupportedMediaType, "json expected")
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	req := &conf.Config{}
	err = json.Unmarshal(b, req)
	if err != nil {
		panic(err)
	}
	cs := s.getConfS(r)
	if err := cs.Save(req); err != nil {
		panic(err)
	}
}
