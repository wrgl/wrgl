// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package local

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	confhelpers "github.com/wrgl/core/pkg/conf/helpers"
)

func TestRepoDirInit(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_repo_dir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	wrglDir := filepath.Join(dir, ".wrgl")
	rd := NewRepoDir(wrglDir, "")
	assert.Equal(t, wrglDir, rd.FullPath)
	assert.False(t, rd.Exist())
	err = rd.Init()
	require.NoError(t, err)
	assert.True(t, rd.Exist())

	_, err = os.Stat(rd.KVPath())
	require.NoError(t, err)
	kvs, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	defer kvs.Close()

	_, err = os.Stat(rd.FilesPath())
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	require.NoError(t, rs.Set("heads/my-branch", []byte("abc123")))
	v, err := rs.Get("heads/my-branch")
	require.NoError(t, err)
	assert.Equal(t, []byte("abc123"), v)
}

func TestFindWrglDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_find_wrgl_dir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	dir, err = filepath.EvalSymlinks(dir)
	require.NoError(t, err)
	home, cleanup := confhelpers.MockHomeDir(t, dir)
	defer cleanup()

	require.NoError(t, os.Chdir(dir))
	p, err := FindWrglDir()
	require.NoError(t, err)
	assert.Empty(t, p)

	wrglDir := filepath.Join(dir, ".wrgl")
	require.NoError(t, os.Mkdir(wrglDir, 0755))
	p, err = FindWrglDir()
	require.NoError(t, err)
	assert.Equal(t, wrglDir, p)

	require.NoError(t, os.Mkdir(filepath.Join(dir, "abc"), 0755))
	require.NoError(t, os.Chdir(filepath.Join(dir, "abc")))
	p, err = FindWrglDir()
	require.NoError(t, err)
	assert.Equal(t, wrglDir, p)

	require.NoError(t, os.Chdir(home))
	p, err = FindWrglDir()
	require.NoError(t, err)
	assert.Empty(t, p)

	require.NoError(t, os.Mkdir(filepath.Join(home, "tmp"), 0755))
	require.NoError(t, os.Chdir(filepath.Join(home, "tmp")))
	p, err = FindWrglDir()
	require.NoError(t, err)
	assert.Empty(t, p)

	wrglDir = filepath.Join(home, ".wrgl")
	require.NoError(t, os.Mkdir(wrglDir, 0755))
	require.NoError(t, os.Chdir(home))
	p, err = FindWrglDir()
	require.NoError(t, err)
	assert.Equal(t, wrglDir, p)
	require.NoError(t, os.Chdir(filepath.Join(home, "tmp")))
	p, err = FindWrglDir()
	require.NoError(t, err)
	assert.Equal(t, wrglDir, p)
}
