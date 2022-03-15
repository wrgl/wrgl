package server_test

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/objects"
	server "github.com/wrgl/wrgl/wrgld/pkg/server"
	server_testutils "github.com/wrgl/wrgl/wrgld/pkg/server/testutils"
)

type testSuite struct {
	s          *server_testutils.Server
	postCommit func(r *http.Request, commit *objects.Commit, sum []byte, branch string)
}

func newSuite(t *testing.T) *testSuite {
	ts := &testSuite{}
	ts.s = server_testutils.NewServer(t, nil, server.WithPostCommitCallback(func(r *http.Request, commit *objects.Commit, sum []byte, branch string) {
		if ts.postCommit != nil {
			ts.postCommit(r, commit, sum, branch)
		}
	}))
	return ts
}

func assertHTTPError(t *testing.T, err error, code int, message string) {
	t.Helper()
	v, ok := err.(*apiclient.HTTPError)
	require.True(t, ok, "error was %v", err)
	assert.Equal(t, code, v.Code)
	assert.Equal(t, message, v.Body.Message)
}

func assertCSVError(t *testing.T, err error, message string, csvLoc *payload.CSVLocation) {
	t.Helper()
	require.IsType(t, &apiclient.HTTPError{}, err, err.Error())
	v := err.(*apiclient.HTTPError)
	assert.Equal(t, http.StatusBadRequest, v.Code)
	assert.Equal(t, message, v.Body.Message)
	assert.Equal(t, csvLoc, v.Body.CSV)
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
