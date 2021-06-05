// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package misc

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuffer(t *testing.T) {
	b := NewBuffer([]byte("abc"))

	n, err := b.Write([]byte("def"))
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	n, err = b.Write([]byte("123"))
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	n, err = b.WriteAt([]byte("qwe"), 15)
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	off, err := b.Seek(12, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(12), off)
	n, err = b.Write([]byte("zx"))
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	off, err = b.Seek(-4, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(10), off)
	n, err = b.Write([]byte{'4'})
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	off, err = b.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(17), off)
	n, err = b.Write([]byte{'5'})
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	assert.Equal(t, "abcdef123\x004\x00zx\x00qw5", string(b.Bytes()))

	sl := b.Buffer(3)
	assert.Equal(t, "abc", string(sl))
	copy(sl, []byte("asd"))
	assert.Equal(t, "asddef123\x004\x00zx\x00qw5", string(b.Bytes()))

	b = NewBuffer(b.Bytes())
	_, err = b.Seek(0, io.SeekStart)
	require.NoError(t, err)
	sl = make([]byte, 3)
	n, err = b.Read(sl)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, "asd", string(sl))
	n, err = b.Read(sl)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, "def", string(sl))
	_, err = b.Seek(-2, io.SeekEnd)
	require.NoError(t, err)
	n, err = b.Read(sl)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, "w5f", string(sl))
}
