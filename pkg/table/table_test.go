package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashTable(t *testing.T) {
	var seed uint64 = 0
	columns := []string{"a", "b", "c"}
	primaryKeyIndices := []int{0}
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
	assert.Equal(t, "d687c6c2440dfefb6661475edef9c11b", sum)

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
	assert.Equal(t, "e066878672e9eb9e1d7380b98319f481", sum)
}
