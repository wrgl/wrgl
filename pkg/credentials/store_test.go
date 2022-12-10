// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package credentials

import (
	"net/url"
	"os"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func mockEnv(t *testing.T, key, val string) func() {
	t.Helper()
	orig := os.Getenv(key)
	require.NoError(t, os.Setenv(key, val))
	return func() {
		require.NoError(t, os.Setenv(key, orig))
	}
}

func mockConfigHome(t *testing.T) func() {
	dir, err := testutils.TempDir("", "test_creds")
	require.NoError(t, err)
	cleanup := mockEnv(t, "XDG_CONFIG_HOME", dir)
	return func() {
		cleanup()
		require.NoError(t, os.RemoveAll(dir))
	}
}

func parseURL(t *testing.T, s string) url.URL {
	t.Helper()
	u, err := url.Parse(s)
	require.NoError(t, err)
	return *u
}

func TestStore(t *testing.T) {
	defer mockConfigHome(t)()
	s, err := NewStore()
	require.NoError(t, err)
	assert.NotEmpty(t, s.Path())

	asURI := parseURL(t, gofakeit.URL())
	accessToken := gofakeit.Sentence(20)
	refreshToken := gofakeit.Sentence(20)
	s.SetAccessToken(asURI, accessToken, refreshToken)
	assert.Equal(t, accessToken, s.GetAccessToken(asURI))
	assert.Equal(t, refreshToken, s.GetRefreshToken(asURI))

	repoURI := parseURL(t, gofakeit.URL())
	rpt := gofakeit.Sentence(20)
	s.SetRPT(repoURI, rpt)
	assert.Equal(t, rpt, s.GetRPT(repoURI))
	uris, err := s.RepoURIs()
	require.NoError(t, err)
	assert.Equal(t, []url.URL{repoURI}, uris)

	require.NoError(t, s.Flush())
	s, err = NewStore()
	require.NoError(t, err)
	assert.Equal(t, accessToken, s.GetAccessToken(asURI))
	assert.Equal(t, refreshToken, s.GetRefreshToken(asURI))
	uris, err = s.RepoURIs()
	require.NoError(t, err)
	assert.Equal(t, []url.URL{repoURI}, uris)
	assert.Equal(t, rpt, s.GetRPT(repoURI))

	assert.True(t, s.DeleteAuthServer(asURI))
	assert.True(t, s.DeleteRepo(repoURI))
	assert.False(t, s.DeleteAuthServer(asURI))
	assert.False(t, s.DeleteRepo(repoURI))

	require.NoError(t, s.Flush())
	s, err = NewStore()
	require.NoError(t, err)
	assert.Empty(t, s.GetAccessToken(asURI))
	assert.Empty(t, s.GetRefreshToken(asURI))
	assert.Empty(t, s.GetRPT(repoURI))
}
