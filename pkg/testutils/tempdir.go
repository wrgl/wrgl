package testutils

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TempDir is a light wrapper of ioutil.Tempdir which place the dir under
// RUNNER_TEMP env var if it is specified. This is necessary for Github action
// to work correctly.
func TempDir(dir, pattern string) (string, error) {
	if v := os.Getenv("RUNNER_TEMP"); v != "" && !strings.HasPrefix(dir, "/") {
		dir = filepath.Join(v, dir)
	}
	return ioutil.TempDir(dir, pattern)
}

// TempFile is a light wrapper of ioutil.TempFile which place the file under
// RUNNER_TEMP env var if it is specified. This is necessary for Github action
// to work correctly.
func TempFile(dir, pattern string) (*os.File, error) {
	if v := os.Getenv("RUNNER_TEMP"); v != "" && !strings.HasPrefix(dir, "/") {
		dir = filepath.Join(v, dir)
	}
	return ioutil.TempFile(dir, pattern)
}

// ChTempDir creates a temporary directory and cd into it during test
func ChTempDir(t *testing.T) (name string, cleanup func()) {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	d, err := TempDir("", "")
	require.NoError(t, err)
	require.NoError(t, os.Chdir(d))
	return d, func() {
		require.NoError(t, os.Chdir(wd))
		require.NoError(t, os.RemoveAll(d))
	}
}
