package table

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockRowHashReader struct {
	rows [][2][]byte
	n    int
}

func (r *MockRowHashReader) Read() (pkHash, rowHash []byte, err error) {
	if r.n >= len(r.rows) {
		return nil, nil, io.EOF
	}
	r.n++
	row := r.rows[r.n-1]
	return row[0], row[1], nil
}

func (r *MockRowHashReader) Close() error {
	return nil
}

func TestHashTable(t *testing.T) {
	var seed uint64 = 0
	columns := []string{"a", "b", "c"}
	primaryKeyIndices := []int{0}
	rowHashReader := &MockRowHashReader{
		rows: [][2][]byte{
			{
				[]byte("abc"),
				[]byte("123"),
			},
			{
				[]byte("def"),
				[]byte("456"),
			},
		},
	}

	sum, err := hashTable(seed, columns, primaryKeyIndices, rowHashReader)
	require.NoError(t, err)
	assert.Equal(t, "d687c6c2440dfefb6661475edef9c11b", sum)

	rowHashReader = &MockRowHashReader{
		rows: [][2][]byte{
			{
				[]byte("abc"),
				[]byte("123"),
			},
		},
	}
	sum, err = hashTable(seed, columns, primaryKeyIndices, rowHashReader)
	require.NoError(t, err)
	assert.Equal(t, "e066878672e9eb9e1d7380b98319f481", sum)
}
