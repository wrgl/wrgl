package credentials

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
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
	dir, err := ioutil.TempDir("", "test_creds")
	require.NoError(t, err)
	cleanup := mockEnv(t, "XDG_CONFIG_HOME", dir)
	return func() {
		cleanup()
		require.NoError(t, os.RemoveAll(dir))
	}
}

func TestStore(t *testing.T) {
	defer mockConfigHome(t)()
	s, err := NewStore()
	require.NoError(t, err)

	m := map[string]string{}
	for i := 0; i < 10; i++ {
		rem := fmt.Sprintf("https://%s.com", testutils.BrokenRandomLowerAlphaString(6))
		tok := testutils.BrokenRandomLowerAlphaString(20)
		m[rem] = tok
		s.Set(rem, tok)
	}
	require.NoError(t, s.Flush())

	s, err = NewStore()
	require.NoError(t, err)
	for rem, tok := range m {
		ts, ok := s.Get(rem)
		assert.True(t, ok)
		assert.Equal(t, tok, ts)
		s.Delete(rem)
	}
	require.NoError(t, s.Flush())

	s, err = NewStore()
	require.NoError(t, err)
	for rem := range m {
		_, ok := s.Get(rem)
		assert.False(t, ok)
	}
}
