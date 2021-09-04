package api_test

import (
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/core/pkg/api/client"
	apiserver "github.com/wrgl/core/pkg/api/server"
	apitest "github.com/wrgl/core/pkg/api/test"
	"github.com/wrgl/core/pkg/auth"
	authtest "github.com/wrgl/core/pkg/auth/test"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/ref"
	refmock "github.com/wrgl/core/pkg/ref/mock"
	"github.com/wrgl/core/pkg/testutils"
)

type testSuite struct {
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
	conf       map[string]*conf.Config
	upSessions map[string]apiserver.UploadPackSessionStore
	rpSessions map[string]apiserver.ReceivePackSessionStore
	s          *apiserver.Server
	postCommit func(commit *objects.Commit, sum []byte, branch string)
}

func newSuite(t *testing.T) *testSuite {
	ts := &testSuite{
		db:         map[string]objects.Store{},
		rs:         map[string]ref.Store{},
		authnS:     map[string]auth.AuthnStore{},
		authzS:     map[string]auth.AuthzStore{},
		conf:       map[string]*conf.Config{},
		upSessions: map[string]apiserver.UploadPackSessionStore{},
		rpSessions: map[string]apiserver.ReceivePackSessionStore{},
	}
	ts.s = apiserver.NewServer(
		ts.getAuthnS, ts.getAuthzS, ts.getDB, ts.getRS, ts.getConf, ts.getUpSessions, ts.getRpSessions,
		apiserver.WithPostCommitCallback(func(commit *objects.Commit, sum []byte, branch string) {
			if ts.postCommit != nil {
				ts.postCommit(commit, sum, branch)
			}
		}),
	)
	return ts
}

func (s *testSuite) getAuthnS(repo string) auth.AuthnStore {
	s.anMx.Lock()
	defer s.anMx.Unlock()
	if _, ok := s.authnS[repo]; !ok {
		s.authnS[repo] = authtest.NewAuthnStore()
	}
	return s.authnS[repo]
}

func (s *testSuite) getAuthzS(repo string) auth.AuthzStore {
	s.azMx.Lock()
	defer s.azMx.Unlock()
	if _, ok := s.authzS[repo]; !ok {
		s.authzS[repo] = authtest.NewAuthzStore()
	}
	return s.authzS[repo]
}

func (s *testSuite) getDB(repo string) objects.Store {
	s.dbMx.Lock()
	defer s.dbMx.Unlock()
	if _, ok := s.db[repo]; !ok {
		s.db[repo] = objmock.NewStore()
	}
	return s.db[repo]
}

func (s *testSuite) getRS(repo string) ref.Store {
	s.rsMx.Lock()
	defer s.rsMx.Unlock()
	if _, ok := s.rs[repo]; !ok {
		s.rs[repo] = refmock.NewStore()
	}
	return s.rs[repo]
}

func (s *testSuite) getConf(repo string) *conf.Config {
	s.cMx.Lock()
	defer s.cMx.Unlock()
	if _, ok := s.conf[repo]; !ok {
		s.conf[repo] = &conf.Config{}
	}
	return s.conf[repo]
}

func (s *testSuite) getUpSessions(repo string) apiserver.UploadPackSessionStore {
	s.upMx.Lock()
	defer s.upMx.Unlock()
	if _, ok := s.upSessions[repo]; !ok {
		s.upSessions[repo] = apiserver.NewUploadPackSessionMap()
	}
	return s.upSessions[repo]
}

func (s *testSuite) getRpSessions(repo string) apiserver.ReceivePackSessionStore {
	s.rpMx.Lock()
	defer s.rpMx.Unlock()
	if _, ok := s.rpSessions[repo]; !ok {
		s.rpSessions[repo] = apiserver.NewReceivePackSessionMap()
	}
	return s.rpSessions[repo]
}

func (s *testSuite) NewClient(t *testing.T, authenticate bool) (string, *apiclient.Client, *requestCaptureMiddleware, func()) {
	t.Helper()
	repo := testutils.BrokenRandomLowerAlphaString(6)
	m := newRequestCaptureMiddleware(&apitest.GZIPAwareHandler{
		T:           t,
		HandlerFunc: s.s.RepoHandler(repo),
	})
	ts := httptest.NewServer(m)
	var opts []apiclient.RequestOption
	if authenticate {
		authnS := s.getAuthnS(repo)
		authzS := s.getAuthzS(repo)
		email := "user@test.com"
		require.NoError(t, authnS.SetPassword(email, "password"))
		require.NoError(t, authzS.AddPolicy(email, auth.ScopeRead))
		require.NoError(t, authzS.AddPolicy(email, auth.ScopeWrite))
		tok, err := authnS.Authenticate(email, "password")
		require.NoError(t, err)
		opts = []apiclient.RequestOption{apiclient.WithAuthorization(tok)}
	}
	cli, err := apiclient.NewClient(ts.URL, opts...)
	require.NoError(t, err)
	return repo, cli, m, ts.Close
}

func TestSuite(t *testing.T) {
	suite := newSuite(t)
	t.Run("", func(t *testing.T) {
		ty := reflect.TypeOf(suite)
		v := reflect.ValueOf(suite)
		for i := ty.NumMethod() - 1; i >= 0; i-- {
			m := ty.Method(i)
			if !strings.HasPrefix(m.Name, "Test") {
				continue
			}
			t.Run(m.Name[4:], func(t *testing.T) {
				t.Parallel()
				v.MethodByName(m.Name).Call([]reflect.Value{reflect.ValueOf(t)})
			})
		}
	})
}
