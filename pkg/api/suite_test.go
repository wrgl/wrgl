package api_test

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	apiserver "github.com/wrgl/core/pkg/api/server"
	apitest "github.com/wrgl/core/pkg/api/test"
	"github.com/wrgl/core/pkg/objects"
)

type testSuite struct {
	s          *apitest.Server
	postCommit func(r *http.Request, commit *objects.Commit, sum []byte, branch string)
}

func newSuite(t *testing.T) *testSuite {
	ts := &testSuite{}
	ts.s = apitest.NewServer(t, nil, apiserver.WithPostCommitCallback(func(r *http.Request, commit *objects.Commit, sum []byte, branch string) {
		if ts.postCommit != nil {
			ts.postCommit(r, commit, sum, branch)
		}
	}))
	return ts
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
