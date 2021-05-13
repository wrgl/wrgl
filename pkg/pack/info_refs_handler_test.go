package pack_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/pack"
	packclient "github.com/wrgl/core/pkg/pack/client"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/pkg/versioning"
)

const (
	testOrigin = "https://wrgl.test"
)

func register(method, path string, handler http.Handler) {
	httpmock.RegisterResponder(method, testOrigin+path,
		func(req *http.Request) (*http.Response, error) {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			resp := rec.Result()
			if resp.Header.Get("Content-Encoding") == "gzip" {
				gzr, err := gzip.NewReader(resp.Body)
				if err != nil {
					return nil, err
				}
				b, err := ioutil.ReadAll(gzr)
				if err != nil {
					return nil, err
				}
				resp.Body = io.NopCloser(bytes.NewReader(b))
				resp.Header.Del("Content-Encoding")
			}
			return resp, nil
		},
	)
}

func TestInfoRefs(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := kv.NewMockStore(false)
	sum1 := testutils.SecureRandomBytes(16)
	head := "my-branch"
	err := versioning.SaveHead(db, head, sum1)
	require.NoError(t, err)
	sum2 := testutils.SecureRandomBytes(16)
	tag := "my-tag"
	err = versioning.SaveTag(db, tag, sum2)
	require.NoError(t, err)
	sum3 := testutils.SecureRandomBytes(16)
	remote := "origin"
	name := "main"
	err = versioning.SaveRemoteRef(db, remote, name, sum3)
	require.NoError(t, err)
	register(http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(db))

	c, err := packclient.NewClient(testOrigin)
	require.NoError(t, err)
	m, err := c.GetRefsInfo()
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"refs/heads/" + head: sum1,
		"refs/tags/" + tag:   sum2,
	}, m)
}
