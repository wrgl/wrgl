package apiserver

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/wrgl/core/pkg/api"
	"github.com/wrgl/core/pkg/conf"
)

func (s *Server) handlePutConfig(rw http.ResponseWriter, r *http.Request) {
	if v := r.Header.Get("Content-Type"); v != api.CTJSON {
		http.Error(rw, "json expected", http.StatusUnsupportedMediaType)
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

}
