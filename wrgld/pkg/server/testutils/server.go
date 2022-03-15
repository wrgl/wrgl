package server_testutils

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/golang-jwt/jwt"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/auth"
	authtest "github.com/wrgl/wrgl/pkg/auth/test"
	"github.com/wrgl/wrgl/pkg/conf"
	confmock "github.com/wrgl/wrgl/pkg/conf/mock"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
	"github.com/wrgl/wrgl/pkg/testutils"
	"github.com/wrgl/wrgl/wrgld/pkg/server"
)

const (
	Email    = "test@user.com"
	Password = "password"
	Name     = "Test User"
)

type Claims struct {
	jwt.StandardClaims
	Email  string   `json:"email,omitempty"`
	Name   string   `json:"name,omitempty"`
	Scopes []string `json:"scopes,omitempty"`
}

type repoKey struct{}

func setRepo(r *http.Request, repo string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), repoKey{}, repo))
}

func getRepo(r *http.Request) string {
	if i := r.Context().Value(repoKey{}); i != nil {
		return i.(string)
	}
	return ""
}

type Server struct {
	dbMx       sync.Mutex
	rsMx       sync.Mutex
	azMx       sync.Mutex
	cMx        sync.Mutex
	upMx       sync.Mutex
	rpMx       sync.Mutex
	db         map[string]objects.Store
	rs         map[string]ref.Store
	authzS     map[string]auth.AuthzStore
	confS      map[string]conf.Store
	upSessions map[string]server.UploadPackSessionStore
	rpSessions map[string]server.ReceivePackSessionStore
	s          *server.Server
}

func NewServer(t *testing.T, rootPath *regexp.Regexp, opts ...server.ServerOption) *Server {
	ts := &Server{
		db:         map[string]objects.Store{},
		rs:         map[string]ref.Store{},
		authzS:     map[string]auth.AuthzStore{},
		confS:      map[string]conf.Store{},
		upSessions: map[string]server.UploadPackSessionStore{},
		rpSessions: map[string]server.ReceivePackSessionStore{},
	}
	ts.s = server.NewServer(
		rootPath,
		func(r *http.Request) objects.Store {
			return ts.GetDB(getRepo(r))
		},
		func(r *http.Request) ref.Store {
			return ts.GetRS(getRepo(r))
		},
		func(r *http.Request) conf.Store {
			return ts.GetConfS(getRepo(r))
		},
		func(r *http.Request) server.UploadPackSessionStore {
			return ts.GetUpSessions(getRepo(r))
		},
		func(r *http.Request) server.ReceivePackSessionStore {
			return ts.GetRpSessions(getRepo(r))
		},
		opts...,
	)
	return ts
}

func (s *Server) GetAuthzS(repo string) auth.AuthzStore {
	s.azMx.Lock()
	defer s.azMx.Unlock()
	if _, ok := s.authzS[repo]; !ok {
		s.authzS[repo] = authtest.NewAuthzStore()
	}
	return s.authzS[repo]
}

func (s *Server) GetDB(repo string) objects.Store {
	s.dbMx.Lock()
	defer s.dbMx.Unlock()
	if _, ok := s.db[repo]; !ok {
		s.db[repo] = objmock.NewStore()
	}
	return s.db[repo]
}

func (s *Server) GetRS(repo string) ref.Store {
	s.rsMx.Lock()
	defer s.rsMx.Unlock()
	if _, ok := s.rs[repo]; !ok {
		s.rs[repo] = refmock.NewStore()
	}
	return s.rs[repo]
}

func (s *Server) GetConfS(repo string) conf.Store {
	s.cMx.Lock()
	defer s.cMx.Unlock()
	if _, ok := s.confS[repo]; !ok {
		s.confS[repo] = &confmock.Store{}
	}
	return s.confS[repo]
}

func (s *Server) GetUpSessions(repo string) server.UploadPackSessionStore {
	s.upMx.Lock()
	defer s.upMx.Unlock()
	if _, ok := s.upSessions[repo]; !ok {
		s.upSessions[repo] = server.NewUploadPackSessionMap()
	}
	return s.upSessions[repo]
}

func (s *Server) GetRpSessions(repo string) server.ReceivePackSessionStore {
	s.rpMx.Lock()
	defer s.rpMx.Unlock()
	if _, ok := s.rpSessions[repo]; !ok {
		s.rpSessions[repo] = server.NewReceivePackSessionMap()
	}
	return s.rpSessions[repo]
}

func (s *Server) Authorize(t *testing.T, email, name string, scopes ...string) (signedToken string) {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodNone, &Claims{
		Email:  email,
		Name:   name,
		Scopes: scopes,
	})
	signedToken, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)
	return
}

func (s *Server) AdminToken(t *testing.T) (signedToken string) {
	return s.Authorize(t, Email, Name, auth.ScopeRepoRead, auth.ScopeRepoWrite)
}

func (s *Server) NewRemote(t *testing.T, pathPrefix string, pathPrefixRegexp *regexp.Regexp) (repo string, url string, m *RequestCaptureMiddleware, cleanup func()) {
	t.Helper()
	repo = testutils.BrokenRandomLowerAlphaString(6)
	cs := s.GetConfS(repo)
	c, err := cs.Open()
	require.NoError(t, err)
	c.User = &conf.User{
		Email: Email,
		Name:  Name,
	}
	require.NoError(t, cs.Save(c))
	m = NewRequestCaptureMiddleware(&GZIPAwareHandler{
		T: t,
		HandlerFunc: func(rw http.ResponseWriter, r *http.Request) {
			r = setRepo(r, repo)
			s.s.ServeHTTP(rw, r)
		},
	})
	var handler http.Handler = ApplyMiddlewares(
		m,
		func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				if s := r.Header.Get("Authorization"); s != "" {
					claims := &Claims{}
					_, err := jwt.ParseWithClaims(
						strings.TrimPrefix(s, "Bearer "), claims,
						func(t *jwt.Token) (interface{}, error) { return jwt.UnsafeAllowNoneSignatureType, nil },
					)
					require.NoError(t, err)
					r = server.SetEmail(server.SetName(r, claims.Name), claims.Email)
				}
				h.ServeHTTP(rw, r)
			})
		},
		server.AuthorizeMiddleware(server.AuthzMiddlewareOptions{
			RootPath: pathPrefixRegexp,
			Enforce: func(r *http.Request, scope string) bool {
				if s := r.Header.Get("Authorization"); s != "" {
					claims := &Claims{}
					_, err := jwt.ParseWithClaims(
						strings.TrimPrefix(s, "Bearer "), claims,
						func(t *jwt.Token) (interface{}, error) { return jwt.UnsafeAllowNoneSignatureType, nil },
					)
					require.NoError(t, err)
					for _, a := range claims.Scopes {
						if a == scope {
							return true
						}
					}
				}
				return false
			},
			GetConfig: func(r *http.Request) *conf.Config {
				c, _ := cs.Open()
				return c
			},
		}),
	)
	if pathPrefix != "" {
		mux := http.NewServeMux()
		mux.Handle(pathPrefix, handler)
		handler = mux
	}
	ts := httptest.NewServer(handler)
	return repo, strings.TrimSuffix(ts.URL+pathPrefix, "/"), m, ts.Close
}

func (s *Server) NewClient(t *testing.T, pathPrefix string, pathPrefixRegexp *regexp.Regexp, authorized bool) (string, *apiclient.Client, *RequestCaptureMiddleware, func()) {
	t.Helper()
	repo, url, m, cleanup := s.NewRemote(t, pathPrefix, pathPrefixRegexp)
	var opts []apiclient.ClientOption
	if authorized {
		opts = append(opts, apiclient.WithAuthorization(s.AdminToken(t)))
	}
	cli, err := apiclient.NewClient(url, opts...)
	require.NoError(t, err)
	return repo, cli, m, cleanup
}
