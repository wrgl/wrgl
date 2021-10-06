package apiserver

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/api/payload"
)

func (s *Server) handleAuthenticate(rw http.ResponseWriter, r *http.Request) {
	if v := r.Header.Get("Content-Type"); v != api.CTJSON {
		sendError(rw, http.StatusUnsupportedMediaType, "json expected")
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	req := &payload.AuthenticateRequest{}
	err = json.Unmarshal(b, req)
	if err != nil {
		panic(err)
	}
	authnS := s.getAuthnS(r)
	tok, err := authnS.Authenticate(req.Email, req.Password)
	if err != nil {
		if err.Error() == "email/password invalid" {
			sendError(rw, http.StatusUnauthorized, err.Error())
			return
		}
		panic(err)
	}
	resp := &payload.AuthenticateResponse{
		IDToken: tok,
	}
	b, err = json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	rw.Header().Set("Content-Type", api.CTJSON)
	_, err = rw.Write(b)
	if err != nil {
		panic(err)
	}
}
