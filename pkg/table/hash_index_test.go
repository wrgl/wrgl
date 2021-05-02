package table

import (
	"bytes"
	"testing"

	"github.com/davecgh/go-spew/spew"
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
	otherHashes := makeBytesMatrix(10, 16)
	for c, rows := range [][][]byte{
		{},
		{testutils.SecureRandomBytes(32)},
		{testutils.SecureRandomBytes(32), testutils.SecureRandomBytes(32)},
		{
			append([]byte{0}, testutils.SecureRandomBytes(31)...),
			append([]byte{128}, testutils.SecureRandomBytes(31)...),
			append([]byte{255}, testutils.SecureRandomBytes(31)...),
		},
		{
			append([]byte{128}, testutils.SecureRandomBytes(31)...),
			append([]byte{0}, testutils.SecureRandomBytes(31)...),
			append([]byte{255}, testutils.SecureRandomBytes(31)...),
			append([]byte{128}, testutils.SecureRandomBytes(31)...),
			append([]byte{255}, testutils.SecureRandomBytes(31)...),
			append([]byte{0}, testutils.SecureRandomBytes(31)...),
		},
		makeBytesMatrix(1024, 32),
	} {
		buf := bytes.NewBuffer(nil)
		w := NewHashIndexWriter(buf, rows)
		require.NoError(t, w.Flush())
		if c == 1 {
			spew.Dump(buf.Bytes())
		}

		i, err := NewHashIndex(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		for j, row := range rows {
			k, err := i.IndexOf(row[:16])
			require.NoError(t, err, "case %d", c)
			assert.Equal(t, j, k, "case %d", c)
		}
		for _, s := range otherHashes {
			k, err := i.IndexOf(s)
			require.NoError(t, err, "case %d", c)
			assert.Equal(t, -1, k, "case %d", c)
		}
	}
}
