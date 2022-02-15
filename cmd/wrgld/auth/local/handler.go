package authlocal

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	wrgldutils "github.com/wrgl/wrgl/cmd/wrgld/utils"
	"github.com/wrgl/wrgl/pkg/api"
	apiserver "github.com/wrgl/wrgl/pkg/api/server"
	"github.com/wrgl/wrgl/pkg/auth"
)

type LocalAuthHandler struct {
	sm     *http.ServeMux
	authnS auth.AuthnStore
}

func NewHandler(handler http.Handler, authnS auth.AuthnStore, authzS auth.AuthzStore) *LocalAuthHandler {
	h := &LocalAuthHandler{
		sm:     http.NewServeMux(),
		authnS: authnS,
	}
	h.sm.HandleFunc("/authenticate/", h.handleAuthenticate)
	h.sm.Handle("/", wrgldutils.ApplyMiddlewares(
		handler,
		apiserver.AuthorizeMiddleware(apiserver.AuthzMiddlewareOptions{
			GetEmailName: func(r *http.Request) (email string, name string) {
				claims := getClaims(r)
				if claims != nil {
					return claims.Email, claims.Name
				}
				return "", ""
			},
			GetScopes: func(r *http.Request) (scopes []string) {
				claims := getClaims(r)
				if claims != nil {
					scopes, err := authzS.ListPolicies(claims.Email)
					if err != nil {
						panic(err)
					}
					return scopes
				}
				return nil
			},
		}),
		AuthenticateMiddleware(authnS),
	))
	return h
}

func (h *LocalAuthHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	h.sm.ServeHTTP(rw, r)
}

func (h *LocalAuthHandler) handleAuthenticate(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		apiserver.SendError(rw, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if v := r.Header.Get("Content-Type"); v != api.CTJSON {
		apiserver.SendError(rw, http.StatusUnsupportedMediaType, "json expected")
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
			apiserver.SendError(rw, http.StatusUnauthorized, err.Error())
			return
		}
		panic(err)
	}
	resp := &AuthenticateResponse{
		IDToken: tok,
	}
	apiserver.WriteJSON(rw, resp)
}
