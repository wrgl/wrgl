package apitest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	apiserver "github.com/wrgl/wrgl/pkg/api/server"
	"github.com/wrgl/wrgl/pkg/auth"
	authtest "github.com/wrgl/wrgl/pkg/auth/test"
	"github.com/wrgl/wrgl/pkg/conf"
	confmock "github.com/wrgl/wrgl/pkg/conf/mock"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
	"github.com/wrgl/wrgl/pkg/testutils"
)

const (
	Email    = "test@user.com"
	Password = "password"
	Name     = "Test User"
)

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
	anMx       sync.Mutex
	azMx       sync.Mutex
	cMx        sync.Mutex
	upMx       sync.Mutex
	rpMx       sync.Mutex
	db         map[string]objects.Store
	rs         map[string]ref.Store
	authnS     map[string]auth.AuthnStore
	authzS     map[string]auth.AuthzStore
	confS      map[string]conf.Store
	upSessions map[string]apiserver.UploadPackSessionStore
	rpSessions map[string]apiserver.ReceivePackSessionStore
	s          *apiserver.Server
}

func NewServer(t *testing.T, rootPath *regexp.Regexp, opts ...apiserver.ServerOption) *Server {
	ts := &Server{
		db:         map[string]objects.Store{},
		rs:         map[string]ref.Store{},
		authnS:     map[string]auth.AuthnStore{},
		authzS:     map[string]auth.AuthzStore{},
		confS:      map[string]conf.Store{},
		upSessions: map[string]apiserver.UploadPackSessionStore{},
		rpSessions: map[string]apiserver.ReceivePackSessionStore{},
	}
	ts.s = apiserver.NewServer(
		rootPath,
		func(r *http.Request) auth.AuthnStore {
			return ts.GetAuthnS(getRepo(r))
		},
		func(r *http.Request) objects.Store {
			return ts.GetDB(getRepo(r))
		},
		func(r *http.Request) ref.Store {
			return ts.GetRS(getRepo(r))
		},
		func(r *http.Request) conf.Store {
			return ts.GetConfS(getRepo(r))
		},
		func(r *http.Request) apiserver.UploadPackSessionStore {
			return ts.GetUpSessions(getRepo(r))
		},
		func(r *http.Request) apiserver.ReceivePackSessionStore {
			return ts.GetRpSessions(getRepo(r))
		},
		opts...,
	)
	return ts
}

func (s *Server) GetAuthnS(repo string) auth.AuthnStore {
	s.anMx.Lock()
	defer s.anMx.Unlock()
	if _, ok := s.authnS[repo]; !ok {
		s.authnS[repo] = authtest.NewAuthnStore()
	}
	return s.authnS[repo]
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

func (s *Server) GetUpSessions(repo string) apiserver.UploadPackSessionStore {
	s.upMx.Lock()
	defer s.upMx.Unlock()
	if _, ok := s.upSessions[repo]; !ok {
		s.upSessions[repo] = apiserver.NewUploadPackSessionMap()
	}
	return s.upSessions[repo]
}

func (s *Server) GetRpSessions(repo string) apiserver.ReceivePackSessionStore {
	s.rpMx.Lock()
	defer s.rpMx.Unlock()
	if _, ok := s.rpSessions[repo]; !ok {
		s.rpSessions[repo] = apiserver.NewReceivePackSessionMap()
	}
	return s.rpSessions[repo]
}

func (s *Server) AddUser(t *testing.T, repo string) {
	t.Helper()
	authnS := s.GetAuthnS(repo)
	authzS := s.GetAuthzS(repo)
	require.NoError(t, authnS.SetPassword(Email, Password))
	for _, s := range []string{
		auth.ScopeRepoRead, auth.ScopeRepoReadConfig, auth.ScopeRepoWrite, auth.ScopeRepoWriteConfig,
	} {
		require.NoError(t, authzS.AddPolicy(Email, s))
	}
}

func (s *Server) NewRemote(t *testing.T, authenticate bool, pathPrefix string, pathPrefixPat *regexp.Regexp) (repo string, url string, m *RequestCaptureMiddleware, cleanup func()) {
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
	handler := apiserver.AuthenticateMiddleware(
		func(r *http.Request) auth.AuthnStore {
			return s.GetAuthnS(repo)
		},
	)(apiserver.AuthorizeMiddleware(
		func(r *http.Request) auth.AuthzStore { return s.GetAuthzS(repo) },
		pathPrefixPat, false,
	)(m))
	if pathPrefix != "" {
		mux := http.NewServeMux()
		mux.Handle(pathPrefix, handler)
		handler = mux
	}
	ts := httptest.NewServer(handler)
	if authenticate {
		s.AddUser(t, repo)
	}
	return repo, strings.TrimSuffix(ts.URL+pathPrefix, "/"), m, ts.Close
}

func (s *Server) GetToken(t *testing.T, repo string) string {
	t.Helper()
	authnS := s.GetAuthnS(repo)
	require.NoError(t, authnS.SetName(Email, Name))
	tok, err := authnS.Authenticate(Email, Password)
	require.NoError(t, err)
	return tok
}

func (s *Server) NewClient(t *testing.T, authenticate bool, pathPrefix string, pathPrefixPat *regexp.Regexp) (string, *apiclient.Client, *RequestCaptureMiddleware, func()) {
	t.Helper()
	repo, url, m, cleanup := s.NewRemote(t, authenticate, pathPrefix, pathPrefixPat)
	var opts []apiclient.RequestOption
	if authenticate {
		tok := s.GetToken(t, repo)
		opts = []apiclient.RequestOption{apiclient.WithAuthorization(tok)}
	}
	cli, err := apiclient.NewClient(url, opts...)
	require.NoError(t, err)
	return repo, cli, m, cleanup
}
