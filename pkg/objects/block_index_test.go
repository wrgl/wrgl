// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/mmcloughlin/meow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func fromHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestBlockIndex(t *testing.T) {
	enc := NewStrListEncoder(true)
	hash := meow.New(0)
	idx, err := IndexBlock(enc, hash, [][]string{
		{"a", "b", "c"},
		{"d", "e", "f"},
		{"g", "h", "k"},
		{"l", "m", "n"},
	}, []uint32{0})
	require.NoError(t, err)
	assert.Equal(t, &BlockIndex{
		sortedOff: []uint8{3, 1, 2, 0},
		Rows: [][]byte{
			fromHex(t, "e5a364171a46ea44556d3cd97e99f3a8eef20ff2a729c390fcac9f0fd26ffdda"),
			fromHex(t, "44f2d02e49bab72155e8319b62c839cc98afafdab4b3d3b689ccbed342fbad61"),
			fromHex(t, "d97ad42f2aba836087cdc10b0f5a5ce479e081c585f5396378c6debd3b3a6944"),
			fromHex(t, "294fca45f7629e5dde21760e232659436773cc7ad69cd3e78c6f08e7709a9449"),
		},
	}, idx)
	for i, row := range idx.Rows {
		j, b := idx.Get(row[:16])
		assert.Equal(t, row[16:], b)
		assert.Equal(t, i, int(j))
	}
	j, b := idx.Get(testutils.SecureRandomBytes(16))
	assert.Nil(t, b)
	assert.Empty(t, j)

	buf := bytes.NewBuffer(nil)
	n, err := idx.WriteTo(buf)
	require.NoError(t, err)
	b = buf.Bytes()
	assert.Len(t, b, int(n))

	n, idx2, err := ReadBlockIndex((bytes.NewReader(b)))
	require.NoError(t, err)
	assert.Equal(t, idx, idx2)
	assert.Len(t, b, int(n))
}

func TestIndexBlockFromBytes(t *testing.T) {
	enc := NewStrListEncoder(true)
	dec := NewStrListDecoder(true)
	buf := bytes.NewBuffer(nil)
	_, err := WriteBlockTo(enc, buf, [][]string{
		{"a", "b", "c"},
		{"d", "e", "f"},
		{"g", "h", "k"},
		{"l", "m", "n"},
	})
	require.NoError(t, err)
	hash := meow.New(0)
	idx, err := IndexBlockFromBytes(dec, hash, buf.Bytes(), []uint32{0})
	require.NoError(t, err)
	assert.Equal(t, &BlockIndex{
		sortedOff: []uint8{3, 1, 2, 0},
		Rows: [][]byte{
			fromHex(t, "e5a364171a46ea44556d3cd97e99f3a8eef20ff2a729c390fcac9f0fd26ffdda"),
			fromHex(t, "44f2d02e49bab72155e8319b62c839cc98afafdab4b3d3b689ccbed342fbad61"),
			fromHex(t, "d97ad42f2aba836087cdc10b0f5a5ce479e081c585f5396378c6debd3b3a6944"),
			fromHex(t, "294fca45f7629e5dde21760e232659436773cc7ad69cd3e78c6f08e7709a9449"),
		},
	}, idx)
	for i, row := range idx.Rows {
		j, b := idx.Get(row[:16])
		assert.Equal(t, row[16:], b)
		assert.Equal(t, i, int(j))
	}
}
