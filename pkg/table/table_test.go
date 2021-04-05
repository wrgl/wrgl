package table

import (
	"encoding/hex"
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
	assert.Equal(t, "9961a6dca881108fa152410e45a3c3d6", hex.EncodeToString(sum))

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
	assert.Equal(t, "731dd4b0ffcfa44a643aa81cf7817d03", hex.EncodeToString(sum))
}
