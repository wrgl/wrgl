package apiserver

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/wrgl/core/pkg/auth"
)

type authenticateMiddleware struct {
	handler   http.Handler
	getAuthnS func(r *http.Request) auth.AuthnStore
}

func AuthenticateMiddleware(handler http.Handler, getAuthnS func(r *http.Request) auth.AuthnStore) http.Handler {
	return &authenticateMiddleware{
		handler:   handler,
		getAuthnS: getAuthnS,
	}
}

func (m *authenticateMiddleware) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if h := r.Header.Get("Authorization"); h != "" && strings.HasPrefix(h, "Bearer ") {
		authnS := m.getAuthnS(r)
		var claims *auth.Claims
		var err error
		r, claims, err = authnS.CheckToken(r, h[7:])
		if err != nil {
			if strings.HasPrefix(err.Error(), "unexpected signing method: ") || err.Error() == "invalid token" {
				http.Error(rw, "invalid token", http.StatusUnauthorized)
				return
			}
			panic(err)
		}
		r = setEmail(r, claims.Email)
	}
	m.handler.ServeHTTP(rw, r)
}

type routeScope struct {
	Pat    *regexp.Regexp
	Method string
	Scope  string
}

type authorizeMiddleware struct {
	handler   http.Handler
	getAuthzS func(r *http.Request) auth.AuthzStore
	routes    []routeScope
}

func AuthorizeMiddleware(handler http.Handler, getAuthzS func(r *http.Request) auth.AuthzStore) http.Handler {
	m := &authorizeMiddleware{
		handler:   handler,
		getAuthzS: getAuthzS,
		routes: []routeScope{
			{
				Pat:    patAuthenticate,
				Method: http.MethodPost,
			},
			{
				Pat:    patConfig,
				Method: http.MethodGet,
				Scope:  auth.ScopeReadConfig,
			},
			{
				Pat:    patConfig,
				Method: http.MethodPut,
				Scope:  auth.ScopeWriteConfig,
			},
			{
				Pat:    patRefsHead,
				Method: http.MethodGet,
				Scope:  auth.ScopeRead,
			},
			{
				Pat:    patRefs,
				Method: http.MethodGet,
				Scope:  auth.ScopeRead,
			},
			{
				Pat:    patUploadPack,
				Method: http.MethodPost,
				Scope:  auth.ScopeRead,
			},
			{
				Pat:    patReceivePack,
				Method: http.MethodPost,
				Scope:  auth.ScopeWrite,
			},
			{
				Pat:    patCommit,
				Method: http.MethodGet,
				Scope:  auth.ScopeRead,
			},
			{
				Pat:    patCommits,
				Method: http.MethodPost,
				Scope:  auth.ScopeWrite,
			},
			{
				Pat:    patCommits,
				Method: http.MethodGet,
				Scope:  auth.ScopeRead,
			},
			{
				Pat:    patTableBlocks,
				Method: http.MethodGet,
				Scope:  auth.ScopeRead,
			},
			{
				Pat:    patTableRows,
				Method: http.MethodGet,
				Scope:  auth.ScopeRead,
			},
			{
				Pat:    patTable,
				Method: http.MethodGet,
				Scope:  auth.ScopeRead,
			},
			{
				Pat:    patDiff,
				Method: http.MethodGet,
				Scope:  auth.ScopeRead,
			},
		},
	}
	return m
}

func (m *authorizeMiddleware) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	var route *routeScope
	for _, o := range m.routes {
		if o.Pat.MatchString(r.URL.Path) && o.Method == r.Method {
			route = &o
			break
		}
	}
	if route == nil {
		http.Error(rw, "not found", http.StatusNotFound)
		return
	}
	if route.Scope != "" {
		authzS := m.getAuthzS(r)
		email := getEmail(r)
		if ok, err := authzS.Authorized(r, email, route.Scope); err != nil {
			panic(err)
		} else if !ok {
			if email == "" {
				http.Error(rw, "unauthorized", http.StatusUnauthorized)
			} else {
				http.Error(rw, "forbidden", http.StatusForbidden)
			}
			return
		}
	}
	m.handler.ServeHTTP(rw, r)
}
