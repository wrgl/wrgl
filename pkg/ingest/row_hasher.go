package ingest

import (
	"fmt"

	"github.com/mmcloughlin/meow"
	"github.com/wrgl/core/pkg/slice"
)

// RowHasher output sum of primary key and hash of row
type RowHasher struct {
	primaryKeyIndices []uint32
	seed              uint64
	encoder           *RowEncoder
}

// NewRowHasher creates a new RowHasher
func NewRowHasher(primaryKeyIndices []uint32, seed uint64) *RowHasher {
	return &RowHasher{
		primaryKeyIndices: primaryKeyIndices,
		seed:              seed,
		encoder:           NewRowEncoder(),
	}
}

// func EncodeRow(record []string) (result []byte, err error) {
// 	return proto.MarshalOptions{Deterministic: true}.Marshal(&objects.Row{Cells: record})
// }

// func DecodeRow(b []byte) (result []string, err error) {
// 	m := new(objects.Row)
// 	err = proto.Unmarshal(b, m)
// 	if err != nil {
// 		return
// 	}
// 	return m.Cells, nil
// }

// Sum calculates sum for pk and row.
func (s *RowHasher) Sum(record []string) (keyHash, rowHash, rowContent []byte, err error) {
	s.encoder.Reset()
	rowContent, err = s.encoder.Encode(record)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encodeStringSlice row: %v", err)
	}
	rowHashArr := meow.Checksum(s.seed, rowContent)
	rowHash = rowHashArr[:]

	var keyContent []byte
	if len(s.primaryKeyIndices) > 0 {
		keyContent, err = s.encoder.Encode(slice.IndicesToValues(record, s.primaryKeyIndices))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("encodeStringSlice pk: %v", err)
		}
	} else {
		keyContent = rowContent
	}
	keyHashArr := meow.Checksum(s.seed, keyContent)
	keyHash = keyHashArr[:]

	return
}
