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

type emailKey struct{}

func SetEmail(r *http.Request, email string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), emailKey{}, email))
}

func GetEmail(r *http.Request) string {
	if i := r.Context().Value(emailKey{}); i != nil {
		return i.(string)
	}
	return ""
}

type nameKey struct{}

func SetName(r *http.Request, name string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), nameKey{}, name))
}

func GetName(r *http.Request) string {
	if i := r.Context().Value(nameKey{}); i != nil {
		return i.(string)
	}
	return ""
}

type AuthzMiddlewareOptions struct {
	RootPath             *regexp.Regexp
	MaskUnauthorizedPath bool
	Enforce              func(r *http.Request, scope string) bool
	RequestScope         func(rw http.ResponseWriter, r *http.Request, scope string)
}

func AuthorizeMiddleware(options AuthzMiddlewareOptions) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			var route *routeScope
			p := r.URL.Path
			if options.RootPath != nil {
				p = strings.TrimPrefix(p, options.RootPath.FindString(p))
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
				SendHTTPError(rw, http.StatusNotFound)
				return
			}
			if route.Scope != "" {
				if options.Enforce(r, route.Scope) {
					handler.ServeHTTP(rw, r)
					return
				}
				if options.RequestScope != nil {
					options.RequestScope(rw, r, route.Scope)
				} else {
					if options.MaskUnauthorizedPath {
						SendHTTPError(rw, http.StatusNotFound)
					} else {
						SendHTTPError(rw, http.StatusForbidden)
					}
				}
			}
		})
	}
}
