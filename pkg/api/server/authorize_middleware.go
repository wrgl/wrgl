package apiserver

import (
	"context"
	"net/http"
	"regexp"
	"strings"

	"github.com/wrgl/wrgl/pkg/auth"
)

type routeScope struct {
	Pat    *regexp.Regexp
	Method string
	Scope  string
}

var routeScopes []routeScope

var (
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

type authzMiddleware struct {
	handler              http.Handler
	rootPath             *regexp.Regexp
	maskUnauthorizedPath bool
	getClaims            func(r *http.Request) *auth.Claims
}

func AuthorizeMiddleware(rootPath *regexp.Regexp, getClaims func(r *http.Request) *auth.Claims, maskUnauthorizedPath bool) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		m := &authzMiddleware{
			handler:              handler,
			rootPath:             rootPath,
			getClaims:            getClaims,
			maskUnauthorizedPath: maskUnauthorizedPath,
		}
		return m
	}
}

func (m *authzMiddleware) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	var route *routeScope
	p := r.URL.Path
	if m.rootPath != nil {
		p = strings.TrimPrefix(p, m.rootPath.FindString(p))
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
		sendHTTPError(rw, http.StatusNotFound)
		return
	}
	if route.Scope != "" {
		claims := m.getClaims(r)
		if claims != nil {
			for _, s := range claims.Scopes {
				if s == route.Scope {
					m.handler.ServeHTTP(rw, setClaims(r, claims))
					return
				}
			}
		}
		if m.maskUnauthorizedPath {
			sendHTTPError(rw, http.StatusNotFound)
		} else {
			sendHTTPError(rw, http.StatusForbidden)
		}
	}
}
