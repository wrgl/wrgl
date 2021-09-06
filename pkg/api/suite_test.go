package api_test

import (
	"reflect"
	"strings"
	"testing"

	apiserver "github.com/wrgl/core/pkg/api/server"
	apitest "github.com/wrgl/core/pkg/api/test"
	"github.com/wrgl/core/pkg/objects"
)

type testSuite struct {
	s          *apitest.Server
	postCommit func(commit *objects.Commit, sum []byte, branch string)
}

func newSuite(t *testing.T) *testSuite {
	ts := &testSuite{}
	ts.s = apitest.NewServer(t, apiserver.WithPostCommitCallback(func(commit *objects.Commit, sum []byte, branch string) {
		if ts.postCommit != nil {
			ts.postCommit(commit, sum, branch)
		}
	}))
	return ts
}

// func (s *testSuite) getAuthnS(r *http.Request) auth.AuthnStore {
// 	return s.s.GetAuthnS(repo)
// }

// func (s *testSuite) getAuthzS(r *http.Request) auth.AuthzStore {
// 	return s.s.GetAuthzS(repo)
// }

// func (s *testSuite) getDB(r *http.Request) objects.Store {
// 	return s.s.GetDB(repo)
// }

// func (s *testSuite) getRS(r *http.Request) ref.Store {
// 	return s.s.GetRS(repo)
// }

// func (s *testSuite) getConf(r *http.Request) *conf.Config {
// 	return s.s.GetConf(repo)
// }

// func (s *testSuite) getUpSessions(r *http.Request) apiserver.UploadPackSessionStore {
// 	return s.s.GetUpSessions(repo)
// }

// func (s *testSuite) getRpSessions(r *http.Request) apiserver.ReceivePackSessionStore {
// 	return s.s.GetRpSessions(repo)
// }

// func (s *testSuite) NewClient(t *testing.T, authenticate bool) (string, *apiclient.Client, *apitest.RequestCaptureMiddleware, func()) {
// 	return s.s.s.NewClient(t, authenticate)
// }

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
