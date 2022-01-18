package apiserver

import (
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/golang-jwt/jwt"
	"github.com/wrgl/wrgl/pkg/auth"
)

type routeScope struct {
	Pat    *regexp.Regexp
	Method string
	Scope  string
}

var routeScopes []routeScope

var (
	patAuthenticate  *regexp.Regexp
	patConfig        *regexp.Regexp
	patRefs          *regexp.Regexp
	patHead          *regexp.Regexp
	patRefsHead      *regexp.Regexp
	patUploadPack    *regexp.Regexp
	patReceivePack   *regexp.Regexp
	patCommits       *regexp.Regexp
	patCommit        *regexp.Regexp
	patSum           *regexp.Regexp
	patProfile       *regexp.Regexp
	patTables        *regexp.Regexp
	patTable         *regexp.Regexp
	patBlocks        *regexp.Regexp
	patTableBlocks   *regexp.Regexp
	patRows          *regexp.Regexp
	patTableRows     *regexp.Regexp
	patDiff          *regexp.Regexp
	patRootedBlocks  *regexp.Regexp
	patRootedRows    *regexp.Regexp
	patCommitProfile *regexp.Regexp
	patTableProfile  *regexp.Regexp
	patObjects       *regexp.Regexp
)

func init() {
	patAuthenticate = regexp.MustCompile(`^/authenticate/`)
	patConfig = regexp.MustCompile(`^/config/`)
	patRefs = regexp.MustCompile(`^/refs/`)
	patHead = regexp.MustCompile(`^heads/[-_0-9a-zA-Z]+/`)
	patRefsHead = regexp.MustCompile(`^/refs/heads/[-_0-9a-zA-Z]+/`)
	patUploadPack = regexp.MustCompile(`^/upload-pack/`)
	patReceivePack = regexp.MustCompile(`^/receive-pack/`)
	patCommits = regexp.MustCompile(`^/commits/`)
	patRootedBlocks = regexp.MustCompile(`^/blocks/`)
	patRootedRows = regexp.MustCompile(`^/rows/`)
	patSum = regexp.MustCompile(`^[0-9a-f]{32}/`)
	patCommit = regexp.MustCompile(`^/commits/[0-9a-f]{32}/`)
	patTables = regexp.MustCompile(`^/tables/`)
	patTable = regexp.MustCompile(`^/tables/[0-9a-f]{32}/`)
	patProfile = regexp.MustCompile(`^profile/`)
	patBlocks = regexp.MustCompile(`^blocks/`)
	patTableBlocks = regexp.MustCompile(`^/tables/[0-9a-f]{32}/blocks/`)
	patRows = regexp.MustCompile(`^rows/`)
	patTableRows = regexp.MustCompile(`^/tables/[0-9a-f]{32}/rows/`)
	patDiff = regexp.MustCompile(`^/diff/[0-9a-f]{32}/[0-9a-f]{32}/`)
	patCommitProfile = regexp.MustCompile(`^/commits/[0-9a-f]{32}/profile/`)
	patTableProfile = regexp.MustCompile(`^/tables/[0-9a-f]{32}/profile/`)
	patObjects = regexp.MustCompile(`^/objects/`)
	routeScopes = []routeScope{
		{
			Pat:    patAuthenticate,
			Method: http.MethodPost,
		},
		{
			Pat:    patConfig,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoReadConfig,
		},
		{
			Pat:    patConfig,
			Method: http.MethodPut,
			Scope:  auth.ScopeRepoWriteConfig,
		},
		{
			Pat:    patRefsHead,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoRead,
		},
		{
			Pat:    patRefs,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoRead,
		},
		{
			Pat:    patUploadPack,
			Method: http.MethodPost,
			Scope:  auth.ScopeRepoRead,
		},
		{
			Pat:    patReceivePack,
			Method: http.MethodPost,
			Scope:  auth.ScopeRepoWrite,
		},
		{
			Pat:    patRootedBlocks,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoRead,
		},
		{
			Pat:    patRootedRows,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoRead,
		},
		{
			Pat:    patObjects,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoRead,
		},
		{
			Pat:    patCommit,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoRead,
		},
		{
			Pat:    patCommitProfile,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoRead,
		},
		{
			Pat:    patCommits,
			Method: http.MethodPost,
			Scope:  auth.ScopeRepoWrite,
		},
		{
			Pat:    patCommits,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoRead,
		},
		{
			Pat:    patTableBlocks,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoRead,
		},
		{
			Pat:    patTableRows,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoRead,
		},
		{
			Pat:    patTable,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoRead,
		},
		{
			Pat:    patTableProfile,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoRead,
		},
		{
			Pat:    patDiff,
			Method: http.MethodGet,
			Scope:  auth.ScopeRepoRead,
		},
	}
}

type authenticateMiddleware struct {
	handler   http.Handler
	getAuthnS func(r *http.Request) auth.AuthnStore
}

func AuthenticateMiddleware(getAuthnS func(r *http.Request) auth.AuthnStore) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return &authenticateMiddleware{
			handler:   handler,
			getAuthnS: getAuthnS,
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
					sendError(rw, http.StatusUnauthorized, "invalid token")
					return
				}
				token = s[7:]
			}
		}
	}
	if token != "" {
		authnS := m.getAuthnS(r)
		var claims *auth.Claims
		var err error
		r, claims, err = authnS.CheckToken(r, token)
		if err != nil {
			if _, ok := err.(*jwt.ValidationError); ok {
				sendError(rw, http.StatusUnauthorized, "invalid token")
				return
			}
			panic(err)
		}
		r = SetClaims(r, claims)
	}
	m.handler.ServeHTTP(rw, r)
}

type authorizeMiddleware struct {
	handler              http.Handler
	getAuthzS            func(r *http.Request) auth.AuthzStore
	pathPrefix           *regexp.Regexp
	maskUnauthorizedPath bool
}

func AuthorizeMiddleware(getAuthzS func(r *http.Request) auth.AuthzStore, pathPrefix *regexp.Regexp, maskUnauthorizedPath bool) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		m := &authorizeMiddleware{
			handler:              handler,
			getAuthzS:            getAuthzS,
			pathPrefix:           pathPrefix,
			maskUnauthorizedPath: maskUnauthorizedPath,
		}
		return m
	}
}

func (m *authorizeMiddleware) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	var route *routeScope
	p := r.URL.Path
	if m.pathPrefix != nil {
		p = strings.TrimPrefix(p, m.pathPrefix.FindString(p))
		if !strings.HasPrefix(p, "/") {
			p = "/" + p
		}
	}
	for _, o := range routeScopes {
		if o.Pat.MatchString(p) && o.Method == r.Method {
			route = &o
			break
		}
	}
	if route == nil {
		sendError(rw, http.StatusNotFound, "not found")
		return
	}
	if route.Scope != "" {
		authzS := m.getAuthzS(r)
		claims := getClaims(r)
		var email string
		if claims != nil {
			email = claims.Email
		}
		if ok, err := authzS.Authorized(r, email, route.Scope); err != nil {
			panic(err)
		} else if !ok {
			if m.maskUnauthorizedPath {
				sendError(rw, http.StatusNotFound, "not found")
			} else if claims == nil {
				sendError(rw, http.StatusUnauthorized, "unauthorized")
			} else {
				sendError(rw, http.StatusForbidden, "forbidden")
			}
			return
		}
	}
	m.handler.ServeHTTP(rw, r)
}
