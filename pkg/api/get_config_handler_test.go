package api_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/core/pkg/api/client"
	"github.com/wrgl/core/pkg/conf"
)

func (s *testSuite) TestGetConfig(t *testing.T) {
	_, cli, m, cleanup := s.s.NewClient(t, true, "", nil)
	defer cleanup()

	cfg := &conf.Config{
		Pack: &conf.Pack{
			MaxFileSize: 1024,
		},
	}
	resp, err := cli.PutConfig(cfg)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	obj, err := cli.GetConfig()
	require.NoError(t, err)
	assert.Equal(t, cfg, obj)

	// pass custom header
	req := m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "123")
		obj, err = cli.GetConfig(apiclient.WithHeader(header))
		require.NoError(t, err)
		assert.NotEmpty(t, obj)
	})
	assert.Equal(t, "123", req.Header.Get("Custom-Header"))
}
