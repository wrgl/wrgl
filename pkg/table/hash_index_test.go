package table

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func makeBytesMatrix(n, m int) [][]byte {
	rows := make([][]byte, n)
	for i := 0; i < len(rows); i++ {
		rows[i] = testutils.SecureRandomBytes(m)
	}
	return rows
}

func TestHashIndex(t *testing.T) {
	rows := makeBytesMatrix(1024, 32)
	buf := bytes.NewBuffer(nil)
	w := NewHashIndexWriter(buf, rows)
	require.NoError(t, w.Flush())

	i, err := NewHashIndex(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	for j, row := range rows {
		k, err := i.IndexOf(row[:16])
		require.NoError(t, err)
		assert.Equal(t, j, k)
	}
	otherHashes := makeBytesMatrix(10, 16)
	for _, s := range otherHashes {
		k, err := i.IndexOf(s)
		require.NoError(t, err)
		assert.Equal(t, -1, k)
	}
}
