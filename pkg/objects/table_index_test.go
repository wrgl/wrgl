package objects

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func TestTableIndex(t *testing.T) {
	idx := NewTableIndex()
	sums := [][]byte{}
	for i := 0; i < 500; i++ {
		sums = append(sums, testutils.SecureRandomBytes(16))
		idx.AddSum(i/128, sums[i])
	}
	fp := 0
	for i := 0; i < 500; i++ {
		if (i / 128) != idx.BlockOffsetOf(sums[i]) {
			fp++
		}
	}
	assert.LessOrEqual(t, fp, 5)

	buf := bytes.NewBuffer(nil)
	n, err := idx.WriteTo(buf)
	require.NoError(t, err)
	b := buf.Bytes()
	assert.Len(t, b, int(n))

	n, idx2, err := ReadTableIndex(bytes.NewReader(b))
	require.NoError(t, err)
	assert.Len(t, b, int(n))
	assert.Equal(t, idx, idx2)
}
