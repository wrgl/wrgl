package testutils

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
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
