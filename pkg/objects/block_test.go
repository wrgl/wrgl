// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func randomBlock(n int) [][]string {
	blk := make([][]string, 255)
	for i := 0; i < 255; i++ {
		blk[i] = make([]string, 10)
		for j := 0; j < 10; j++ {
			blk[i][j] = testutils.BrokenRandomAlphaNumericString(5)
		}
	}
	return blk
}

func TestWriteBlockTo(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	blk := randomBlock(255)
	enc := NewStrListEncoder(true)
	n, err := WriteBlockTo(enc, buf, blk)
	require.NoError(t, err)
	b := buf.Bytes()
	assert.Len(t, b, int(n))
	m, blk2, err := ReadBlockFrom(bytes.NewReader(b))
	require.NoError(t, err)
	assert.Len(t, b, int(m))
	assert.Equal(t, blk, blk2)
}

func TestCombineRowBytesIntoBlock(t *testing.T) {
	enc := NewStrListEncoder(false)
	for _, n := range []int{255, 100} {
		blk := randomBlock(n)
		sl := make([][]byte, len(blk))
		for i, row := range blk {
			sl[i] = enc.Encode(row)
		}
		b := CombineRowBytesIntoBlock(sl)
		m, blk2, err := ReadBlockFrom(bytes.NewReader(b))
		require.NoError(t, err)
		assert.Len(t, b, int(m))
		assert.Equal(t, len(blk), len(blk2))
		assert.Equal(t, blk, blk2)
	}
}
