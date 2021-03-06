// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package local

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fsnotify/fsnotify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	confhelpers "github.com/wrgl/wrgl/pkg/conf/helpers"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestRepoDirInit(t *testing.T) {
	dir, err := testutils.TempDir("", "test_repo_dir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	wrglDir := filepath.Join(dir, ".wrgl")
	rd, err := NewRepoDir(wrglDir, "")
	require.NoError(t, err)
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

	rs := rd.OpenRefStore()
	require.NoError(t, rs.Set("heads/my-branch", []byte("abc123")))
	v, err := rs.Get("heads/my-branch")
	require.NoError(t, err)
	assert.Equal(t, []byte("abc123"), v)
}

func TestFindWrglDir(t *testing.T) {
	dir, err := testutils.TempDir("", "test_find_wrgl_dir")
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

func TestRepoDirWatcher(t *testing.T) {
	dir, err := testutils.TempDir("", "test_repo_dir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	rd, err := NewRepoDir(dir, "")
	require.NoError(t, err)
	defer rd.Close()

	w, err := rd.Watcher()
	require.NoError(t, err)
	fp := filepath.Join(dir, "abc.txt")
	f, err := os.Create(fp)
	require.NoError(t, err)
	_, err = f.Write([]byte("def"))
	require.NoError(t, err)
	require.NoError(t, err)
	event := <-w.Events
	assert.Equal(t, fsnotify.Event{
		Name: fp,
		Op:   fsnotify.Create,
	}, event)
}
