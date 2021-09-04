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

// func (s *testSuite) getAuthnS(repo string) auth.AuthnStore {
// 	return s.s.GetAuthnS(repo)
// }

// func (s *testSuite) getAuthzS(repo string) auth.AuthzStore {
// 	return s.s.GetAuthzS(repo)
// }

// func (s *testSuite) getDB(repo string) objects.Store {
// 	return s.s.GetDB(repo)
// }

// func (s *testSuite) getRS(repo string) ref.Store {
// 	return s.s.GetRS(repo)
// }

// func (s *testSuite) getConf(repo string) *conf.Config {
// 	return s.s.GetConf(repo)
// }

// func (s *testSuite) getUpSessions(repo string) apiserver.UploadPackSessionStore {
// 	return s.s.GetUpSessions(repo)
// }

// func (s *testSuite) getRpSessions(repo string) apiserver.ReceivePackSessionStore {
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
