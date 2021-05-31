// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"fmt"
	"io"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
)

type RowChangeReader struct {
	Columns  *objects.ColDiff
	rowPairs [][2][]byte
	off      int
	db1, db2 kv.DB
	dec      *objects.StrListDecoder
}

func NewRowChangeReader(db1, db2 kv.DB, colDiff *objects.ColDiff) (*RowChangeReader, error) {
	return &RowChangeReader{
		db1:     db1,
		db2:     db2,
		Columns: colDiff,
		dec:     objects.NewStrListDecoder(false),
	}, nil
}

func (r *RowChangeReader) AddRowPair(row, oldRow []byte) {
	r.rowPairs = append(r.rowPairs, [2][]byte{row, oldRow})
}

func (r *RowChangeReader) Read() ([][]string, error) {
	mergedRow, err := r.ReadAt(r.off)
	if err != nil {
		return nil, err
	}
	r.off++
	return mergedRow, nil
}

func (r *RowChangeReader) NumRows() int {
	return len(r.rowPairs)
}

func (r *RowChangeReader) Seek(offset int, whence int) (int, error) {
	switch whence {
	default:
		return 0, fmt.Errorf("Seek: invalid whence")
	case io.SeekStart:
		break
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += len(r.rowPairs)
	}
	if offset < 0 {
		return 0, fmt.Errorf("Seek: invalid offset")
	}
	r.off = offset
	return offset, nil
}

func fetchRow(db kv.DB, row []byte, dec *objects.StrListDecoder) ([]string, error) {
	b, err := table.GetRow(db, row)
	if err != nil {
		return nil, err
	}
	return dec.Decode(b), nil
}

func (r *RowChangeReader) ReadAt(offset int) (mergedRow [][]string, err error) {
	if offset >= len(r.rowPairs) {
		return nil, io.EOF
	}
	pair := r.rowPairs[offset]
	row, err := fetchRow(r.db1, pair[0], r.dec)
	if err != nil {
		return nil, err
	}
	oldRow, err := fetchRow(r.db2, pair[1], r.dec)
	if err != nil {
		return nil, err
	}
	return r.Columns.CombineRows(0, row, oldRow), nil
}
