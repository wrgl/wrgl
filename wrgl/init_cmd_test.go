package main

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitCmd(t *testing.T) {
	rootDir, err := ioutil.TempDir("", "test_wrgl_*")
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)
	cmd := newRootCmd()
	cmd.SetArgs([]string{"init", "--root-dir", rootDir})
	cmd.SetOut(io.Discard)
	err = cmd.Execute()
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(rootDir, ".wrgl", "files"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(rootDir, ".wrgl", "kv"))
	require.NoError(t, err)
}
