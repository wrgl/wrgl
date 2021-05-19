// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package kv

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setFileStore(t *testing.T, s FileStore, k, v []byte) {
	t.Helper()
	w, err := s.Writer(k)
	require.NoError(t, err)
	defer w.Close()
	_, err = w.Write(v)
	require.NoError(t, err)
}

func getFileStore(s FileStore, k []byte) ([]byte, error) {
	r, err := s.Reader(k)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}

func TestFileStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "data")
	require.NoError(t, err)
	err = os.MkdirAll(dir, 0644)
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s := NewFileStore(dir)
	setFileStore(t, s, []byte("proj1/abc"), []byte("123"))
	setFileStore(t, s, []byte("proj2/asd"), []byte("345"))
	setFileStore(t, s, []byte("proj2/qwe"), []byte("678"))

	var v []byte
	v, err = getFileStore(s, []byte("proj1/abc"))
	require.NoError(t, err)
	assert.Equal(t, []byte("123"), v)

	size, err := s.Size([]byte("proj1/abc"))
	require.NoError(t, err)
	assert.Equal(t, uint64(3), size)

	v, err = getFileStore(s, []byte("sdf"))
	assert.Equal(t, KeyNotFoundError, err)
	assert.Nil(t, v)

	assert.True(t, s.Exist([]byte("proj2/asd")))
	assert.True(t, s.Exist([]byte("proj2/qwe")))

	r, err := s.Reader([]byte("proj2/asd"))
	require.NoError(t, err)
	b, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, []byte("345"), b)

	err = s.Delete([]byte("proj2/asd"))
	require.NoError(t, err)
	assert.False(t, s.Exist([]byte("proj2/asd")))

	err = s.Clear([]byte("proj2"))
	require.NoError(t, err)
	assert.False(t, s.Exist([]byte("proj2/qwe")))

	// test move
	err = s.Move([]byte("proj1/abc"), []byte("proj0/abc"))
	require.NoError(t, err)
	assert.False(t, s.Exist([]byte("proj1/abc")))
	v, err = getFileStore(s, []byte("proj0/abc"))
	require.NoError(t, err)
	assert.Equal(t, []byte("123"), v)

	// test AppendWriter
	w, err := s.AppendWriter([]byte("proj0/abc"))
	require.NoError(t, err)
	_, err = w.Write([]byte("456"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	v, err = getFileStore(s, []byte("proj0/abc"))
	require.NoError(t, err)
	assert.Equal(t, []byte("123456"), v)
}
