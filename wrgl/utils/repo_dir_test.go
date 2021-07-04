// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package utils

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepoDirInit(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_repo_dir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	wrglDir := filepath.Join(dir, ".wrgl")
	rd := NewRepoDir(wrglDir, false, false)
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
