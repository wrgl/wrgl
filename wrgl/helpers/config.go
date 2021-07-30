package wrglhelpers

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func MockEnv(t *testing.T, key, val string) func() {
	t.Helper()
	orig := os.Getenv(key)
	require.NoError(t, os.Setenv(key, val))
	return func() {
		require.NoError(t, os.Setenv(key, orig))
	}
}

func MockGlobalConf(t *testing.T, setXDGConfigHome bool) func() {
	t.Helper()
	name, err := ioutil.TempDir("", "test_wrgl_config")
	require.NoError(t, err)
	var cleanup1, cleanup2 func()
	if setXDGConfigHome {
		cleanup1 = MockEnv(t, "XDG_CONFIG_HOME", name)
	} else {
		cleanup1 = MockEnv(t, "XDG_CONFIG_HOME", "")
		cleanup2 = MockEnv(t, "HOME", name)
	}
	return func() {
		require.NoError(t, os.RemoveAll(name))
		cleanup1()
		if cleanup2 != nil {
			cleanup2()
		}
	}
}

func MockSystemConf(t *testing.T) func() {
	t.Helper()
	dir, err := ioutil.TempDir("", "test_wrgl_config")
	require.NoError(t, err)
	cleanup := MockEnv(t, "WRGL_SYSTEM_CONFIG_DIR", dir)
	return func() {
		require.NoError(t, os.RemoveAll(dir))
		cleanup()
	}
}
