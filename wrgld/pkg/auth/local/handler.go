package authlocal

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/auth"
	"github.com/wrgl/wrgl/pkg/conf"
	server "github.com/wrgl/wrgl/wrgld/pkg/server"
	wrgldutils "github.com/wrgl/wrgl/wrgld/pkg/utils"
)

type LocalAuthHandler struct {
	sm     *http.ServeMux
	authnS auth.AuthnStore
}

func NewHandler(handler http.Handler, c *conf.Config, authnS auth.AuthnStore, authzS auth.AuthzStore) *LocalAuthHandler {
	h := &LocalAuthHandler{
		sm:     http.NewServeMux(),
		authnS: authnS,
	}
	h.sm.HandleFunc("/authenticate/", h.handleAuthenticate)
	h.sm.Handle("/", wrgldutils.ApplyMiddlewares(
		handler,
		func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				claims := getClaims(r)
				if claims != nil {
					r = server.SetEmail(server.SetName(r, claims.Name), claims.Email)
				}
				h.ServeHTTP(rw, r)
			})
		},
		server.AuthorizeMiddleware(server.AuthzMiddlewareOptions{
			Enforce: func(r *http.Request, scope string) bool {
				claims := getClaims(r)
				if claims != nil {
					ok, err := authzS.Authorized(r, claims.Email, scope)
					if err != nil {
						panic(err)
					}
					return ok
				}
				return false
			},
			GetConfig: func(r *http.Request) *conf.Config {
				return c
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
		server.SendError(rw, r, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if v := r.Header.Get("Content-Type"); v != api.CTJSON {
		server.SendError(rw, r, http.StatusUnsupportedMediaType, "json expected")
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
			server.SendError(rw, r, http.StatusUnauthorized, err.Error())
			return
		}
		panic(err)
	}
	resp := &AuthenticateResponse{
		IDToken: tok,
	}
	server.WriteJSON(rw, r, resp)
}
