package ingest

import (
	"fmt"

	"github.com/mmcloughlin/meow"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/slice"
)

// RowHasher output sum of primary key and hash of row
type RowHasher struct {
	primaryKeyIndices []uint32
	seed              uint64
	encoder           *objects.StrListEncoder
}

// NewRowHasher creates a new RowHasher
func NewRowHasher(primaryKeyIndices []uint32, seed uint64) *RowHasher {
	return &RowHasher{
		primaryKeyIndices: primaryKeyIndices,
		seed:              seed,
		encoder:           objects.NewStrListEncoder(),
	}
}

// Sum calculates sum for pk and row.
func (s *RowHasher) Sum(record []string) (keyHash, rowHash, rowContent []byte, err error) {
	b := s.encoder.Encode(record)
	rowContent = make([]byte, len(b))
	copy(rowContent, b)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encodeStringSlice row: %v", err)
	}
	rowHashArr := meow.Checksum(s.seed, rowContent)
	rowHash = rowHashArr[:]

	var keyContent []byte
	if len(s.primaryKeyIndices) > 0 {
		keyContent = s.encoder.Encode(slice.IndicesToValues(record, s.primaryKeyIndices))
	} else {
		keyContent = rowContent
	}
	keyHashArr := meow.Checksum(s.seed, keyContent)
	keyHash = keyHashArr[:]

	return
}
