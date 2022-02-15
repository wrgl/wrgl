package authlocal

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"github.com/golang-jwt/jwt"
	apiserver "github.com/wrgl/wrgl/pkg/api/server"
	"github.com/wrgl/wrgl/pkg/auth"
)

type claimsKey struct{}

func setClaims(r *http.Request, claims *auth.Claims) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), claimsKey{}, claims))
}

func getClaims(r *http.Request) *auth.Claims {
	if i := r.Context().Value(claimsKey{}); i != nil {
		return i.(*auth.Claims)
	}
	return nil
}

type authenticateMiddleware struct {
	handler http.Handler
	authnS  auth.AuthnStore
}

func AuthenticateMiddleware(authnS auth.AuthnStore) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return &authenticateMiddleware{
			handler: handler,
			authnS:  authnS,
		}
	}
}

func (m *authenticateMiddleware) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	var token string
	if h := r.Header.Get("Authorization"); h != "" && strings.HasPrefix(h, "Bearer ") {
		token = h[7:]
	}
	if token == "" && r.Method == http.MethodGet {
		cookie, err := r.Cookie("Authorization")
		if err == nil {
			if strings.HasPrefix(cookie.Value, "Bearer ") {
				token = cookie.Value[7:]
			} else if strings.HasPrefix(cookie.Value, "Bearer%20") || strings.HasPrefix(cookie.Value, "Bearer+") {
				s, err := url.QueryUnescape(cookie.Value)
				if err != nil {
					apiserver.SendError(rw, http.StatusUnauthorized, "invalid token")
					return
				}
				token = s[7:]
			}
		}
	}
	if token != "" {
		var claims *auth.Claims
		var err error
		r, claims, err = m.authnS.CheckToken(r, token)
		if err != nil {
			if _, ok := err.(*jwt.ValidationError); ok {
				apiserver.SendError(rw, http.StatusUnauthorized, "invalid token")
				return
			}
			panic(err)
		}
		r = setClaims(r, claims)
	}
	m.handler.ServeHTTP(rw, r)
}
