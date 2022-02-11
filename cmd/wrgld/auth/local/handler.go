package authlocal

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"regexp"

	wrgldutils "github.com/wrgl/wrgl/cmd/wrgld/utils"
	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/api/payload"
	apiserver "github.com/wrgl/wrgl/pkg/api/server"
	"github.com/wrgl/wrgl/pkg/auth"
)

type LocalAuthHandler struct {
	sm     *http.ServeMux
	authnS auth.AuthnStore
}

func NewHandler(handler http.Handler, rootPath *regexp.Regexp, authnS auth.AuthnStore, authzS auth.AuthzStore, maskUnauthorizedPath bool) *LocalAuthHandler {
	h := &LocalAuthHandler{
		sm:     http.NewServeMux(),
		authnS: authnS,
	}
	h.sm.HandleFunc("/authenticate/", h.handleAuthenticate)
	h.sm.Handle("/", wrgldutils.ApplyMiddlewares(
		handler,
		apiserver.AuthorizeMiddleware(rootPath, func(r *http.Request) *auth.Claims {
			claims := getClaims(r)
			if claims != nil {
				scopes, err := authzS.ListPolicies(claims.Email)
				if err != nil {
					panic(err)
				}
				claims.Scopes = scopes
			}
			return claims
		}, maskUnauthorizedPath),
		AuthenticateMiddleware(authnS),
	))
	return h
}

func (h *LocalAuthHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	h.sm.ServeHTTP(rw, r)
}

func sendError(rw http.ResponseWriter, code int, message string) {
	rw.Header().Set("Content-Type", api.CTJSON)
	rw.WriteHeader(code)
	b, err := json.Marshal(&payload.Error{
		Message: message,
	})
	if err != nil {
		panic(err)
	}
	_, err = rw.Write(b)
	if err != nil {
		panic(err)
	}
}

func (h *LocalAuthHandler) handleAuthenticate(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendError(rw, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if v := r.Header.Get("Content-Type"); v != api.CTJSON {
		sendError(rw, http.StatusUnsupportedMediaType, "json expected")
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	req := &AuthenticateRequest{}
	err = json.Unmarshal(b, req)
	if err != nil {
		panic(err)
	}
	tok, err := h.authnS.Authenticate(req.Email, req.Password)
	if err != nil {
		if err.Error() == "email/password invalid" {
			sendError(rw, http.StatusUnauthorized, err.Error())
			return
		}
		panic(err)
	}
	resp := &AuthenticateResponse{
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
