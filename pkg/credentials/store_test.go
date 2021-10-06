package credentials

import (
	"fmt"
	"net/url"
	"os"
	"testing"

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

	m := map[string]string{}
	for i := 0; i < 10; i++ {
		rem := fmt.Sprintf("https://%s.com", testutils.BrokenRandomLowerAlphaString(6))
		tok := testutils.BrokenRandomLowerAlphaString(20)
		m[rem] = tok
		s.Set(parseURL(t, rem), tok)
	}
	assert.Equal(t, 10, s.Len())
	for rem, tok := range m {
		s.Set(parseURL(t, rem), tok)
	}
	assert.Equal(t, 10, s.Len())
	uris := s.URIs()
	for _, u := range uris {
		_, ok := m[u.String()]
		assert.True(t, ok)
	}
	require.NoError(t, s.Flush())

	s, err = NewStore()
	require.NoError(t, err)
	for rem, ts := range m {
		uri, tok := s.GetTokenMatching(parseURL(t, rem))
		require.NotNil(t, uri)
		assert.Equal(t, rem, uri.String())
		assert.Equal(t, ts, tok)
		assert.True(t, s.Delete(*uri))
		assert.False(t, s.Delete(*uri))
	}
	require.NoError(t, s.Flush())

	s, err = NewStore()
	require.NoError(t, err)
	for rem := range m {
		uri, tok := s.GetTokenMatching(parseURL(t, rem))
		assert.Nil(t, uri)
		assert.Empty(t, tok)
	}
}

func TestSelectLongestPrefix(t *testing.T) {
	defer mockConfigHome(t)()
	s, err := NewStore()
	require.NoError(t, err)

	tokens := make([]string, 5)
	for i := range tokens {
		tokens[i] = testutils.BrokenRandomAlphaNumericString(20)
	}
	s.Set(parseURL(t, "https://my-host2.com"), tokens[0])
	s.Set(parseURL(t, "https://my-host2.com/repos/repo1"), tokens[1])
	s.Set(parseURL(t, "https://my-host2.com/repos/repo2"), tokens[2])
	s.Set(parseURL(t, "https://my-host3.com"), tokens[3])
	s.Set(parseURL(t, "https://my-host1.com"), tokens[4])

	u, tok := s.GetTokenMatching(parseURL(t, "https://my-host1.com"))
	assert.Equal(t, "https://my-host1.com", u.String())
	assert.Equal(t, tokens[4], tok)

	u, tok = s.GetTokenMatching(parseURL(t, "https://my-host3.com/abc/edf/"))
	assert.Equal(t, "https://my-host3.com", u.String())
	assert.Equal(t, tokens[3], tok)

	u, tok = s.GetTokenMatching(parseURL(t, "https://my-host2.com/repos/repo1"))
	assert.Equal(t, "https://my-host2.com/repos/repo1", u.String())
	assert.Equal(t, tokens[1], tok)

	u, tok = s.GetTokenMatching(parseURL(t, "https://my-host2.com/repos/repo2/abc/"))
	assert.Equal(t, "https://my-host2.com/repos/repo2", u.String())
	assert.Equal(t, tokens[2], tok)

	u, tok = s.GetTokenMatching(parseURL(t, "https://my-host2.com/repos/repo3"))
	assert.Equal(t, "https://my-host2.com", u.String())
	assert.Equal(t, tokens[0], tok)
}
