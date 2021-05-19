// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package table

import (
	"encoding/hex"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
)

func TestKeyListRowReader(t *testing.T) {
	db := kv.NewMockStore(false)
	k1 := []byte("a")
	r1 := []byte("1,2,3")
	k2 := []byte("b")
	r2 := []byte("4,5,6")
	k3 := []byte("c")
	r3 := []byte("7,8,9")
	require.NoError(t, SaveRow(db, k1, r1))
	require.NoError(t, SaveRow(db, k2, r2))
	require.NoError(t, SaveRow(db, k3, r3))
	keys := []string{
		hex.EncodeToString(k1),
		hex.EncodeToString(k2),
		hex.EncodeToString(k3),
	}

	// test read
	reader := NewKeyListRowReader(db, keys)
	assert.Equal(t, 3, reader.NumRows())
	assertRowRead(t, reader, k1, r1)
	assertRowRead(t, reader, k2, r2)
	assertRowRead(t, reader, k3, r3)
	_, _, err := reader.Read()
	assert.Equal(t, io.EOF, err)
	reader.Add(hex.EncodeToString(k1))
	assert.Equal(t, 4, reader.NumRows())
	assertRowRead(t, reader, k1, r1)

	// test Seek
	reader = NewKeyListRowReader(db, keys)
	off, err := reader.Seek(2, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, 2, off)
	assertRowRead(t, reader, k3, r3)
	off, err = reader.Seek(-2, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, 1, off)
	assertRowRead(t, reader, k2, r2)
	off, err = reader.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, 2, off)
	assertRowRead(t, reader, k3, r3)

	// test ReadAt
	reader = NewKeyListRowReader(db, keys)
	rh, rc, err := reader.ReadAt(1)
	require.NoError(t, err)
	assert.Equal(t, k2, rh)
	assert.Equal(t, r2, rc)
	assertRowRead(t, reader, k1, r1)
}
