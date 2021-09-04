package apitest

import (
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/core/pkg/api/client"
	apiserver "github.com/wrgl/core/pkg/api/server"
	"github.com/wrgl/core/pkg/auth"
	authtest "github.com/wrgl/core/pkg/auth/test"
	"github.com/wrgl/core/pkg/conf"
	confmock "github.com/wrgl/core/pkg/conf/mock"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/ref"
	refmock "github.com/wrgl/core/pkg/ref/mock"
	"github.com/wrgl/core/pkg/testutils"
)

const (
	Email    = "test@user.com"
	Password = "password"
)

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

func NewServer(t *testing.T, opts ...apiserver.ServerOption) *Server {
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
		ts.GetAuthnS, ts.GetAuthzS, ts.GetDB, ts.GetRS, ts.GetConfS, ts.GetUpSessions, ts.GetRpSessions, opts...,
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

func (s *Server) NewRemote(t *testing.T, authenticate bool) (repo string, url string, m *RequestCaptureMiddleware, cleanup func()) {
	t.Helper()
	repo = testutils.BrokenRandomLowerAlphaString(6)
	m = NewRequestCaptureMiddleware(&GZIPAwareHandler{
		T:           t,
		HandlerFunc: s.s.RepoHandler(repo),
	})
	ts := httptest.NewServer(m)
	if authenticate {
		authnS := s.GetAuthnS(repo)
		authzS := s.GetAuthzS(repo)
		require.NoError(t, authnS.SetPassword(Email, Password))
		for _, s := range []string{
			auth.ScopeRead, auth.ScopeReadConfig, auth.ScopeWrite, auth.ScopeWriteConfig,
		} {
			require.NoError(t, authzS.AddPolicy(Email, s))
		}
	}
	return repo, ts.URL, m, ts.Close
}

func (s *Server) NewClient(t *testing.T, authenticate bool) (string, *apiclient.Client, *RequestCaptureMiddleware, func()) {
	t.Helper()
	repo, url, m, cleanup := s.NewRemote(t, authenticate)
	var opts []apiclient.RequestOption
	if authenticate {
		authnS := s.GetAuthnS(repo)
		tok, err := authnS.Authenticate(Email, Password)
		require.NoError(t, err)
		opts = []apiclient.RequestOption{apiclient.WithAuthorization(tok)}
	}
	cli, err := apiclient.NewClient(url, opts...)
	require.NoError(t, err)
	return repo, cli, m, cleanup
}
