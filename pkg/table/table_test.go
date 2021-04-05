package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashTable(t *testing.T) {
	var seed uint64 = 0
	columns := []string{"a", "b", "c"}
	primaryKeyIndices := []uint32{0}
	rowHashReader := &MockRowHashReader{
		rows: [][2]string{
			{
				"abc",
				"123",
			},
			{
				"def",
				"456",
			},
		},
	}

	sum, err := hashTable(seed, columns, primaryKeyIndices, rowHashReader)
	require.NoError(t, err)
	assert.Equal(t, "bf1096ae00c76254772641a02c221db0", sum)

	rowHashReader = &MockRowHashReader{
		rows: [][2]string{
			{
				"abc",
				"123",
			},
		},
	}
	sum, err = hashTable(seed, columns, primaryKeyIndices, rowHashReader)
	require.NoError(t, err)
	assert.Equal(t, "a2a7caaa151575dbe5f413f53ee002eb", sum)
}
