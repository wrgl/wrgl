package ingest

import (
	"fmt"

	"github.com/mmcloughlin/meow"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/slice"
)

// RowHasher output sum of primary key and hash of row
type RowHasher struct {
	primaryKeyIndices []int
	seed              uint64
}

// NewRowHasher creates a new RowHasher
func NewRowHasher(primaryKeyIndices []int, seed uint64) *RowHasher {
	return &RowHasher{
		primaryKeyIndices: primaryKeyIndices,
		seed:              seed,
	}
}

// Sum calculates sum for pk and row.
func (s *RowHasher) Sum(record []string) (keyHash, rowHash, rowContent []byte, err error) {
	rowContent, err = encoding.EncodeStrings(record)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encodeStringSlice row: %v", err)
	}

	var keyContent []byte
	if len(s.primaryKeyIndices) > 0 {
		keyContent, err = encoding.EncodeStrings(slice.IndicesToValues(record, s.primaryKeyIndices))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("encodeStringSlice pk: %v", err)
		}
	} else {
		keyContent = rowContent
	}

	keyHashArr := meow.Checksum(s.seed, keyContent)
	keyHash = keyHashArr[:]
	rowHashArr := meow.Checksum(s.seed, rowContent)
	rowHash = rowHashArr[:]

	return
}
