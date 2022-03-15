// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package diff

import (
	"fmt"
	"io"

	"github.com/wrgl/wrgl/pkg/objects"
)

type RowChangeReader struct {
	ColDiff  *ColDiff
	rowDiffs []*objects.Diff
	off      int
	buf      *BlockBuffer
}

func NewRowChangeReader(db1, db2 objects.Store, tbl1, tbl2 *objects.Table, colDiff *ColDiff) (*RowChangeReader, error) {
	buf, err := NewBlockBuffer([]objects.Store{db1, db2}, []*objects.Table{tbl1, tbl2})
	if err != nil {
		return nil, err
	}
	return &RowChangeReader{
		buf:     buf,
		ColDiff: colDiff,
	}, nil
}

func (r *RowChangeReader) AddRowDiff(d *objects.Diff) {
	r.rowDiffs = append(r.rowDiffs, d)
}

func (r *RowChangeReader) Read() ([][]string, error) {
	mergedRow, err := r.ReadAt(r.off)
	if err != nil {
		return nil, err
	}
	r.off++
	return mergedRow, nil
}

func (r *RowChangeReader) Len() int {
	if r == nil {
		return 0
	}
	return len(r.rowDiffs)
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
		offset += len(r.rowDiffs)
	}
	if offset < 0 {
		return 0, fmt.Errorf("Seek: invalid offset")
	}
	r.off = offset
	return offset, nil
}

func (r *RowChangeReader) ReadAt(offset int) (mergedRow [][]string, err error) {
	if offset >= len(r.rowDiffs) {
		return nil, io.EOF
	}
	d := r.rowDiffs[offset]
	blk, off := RowToBlockAndOffset(d.Offset)
	row, err := r.buf.GetRow(0, blk, off)
	if err != nil {
		return nil, err
	}
	blk, off = RowToBlockAndOffset(d.OldOffset)
	oldRow, err := r.buf.GetRow(1, blk, off)
	if err != nil {
		return nil, err
	}
	return r.ColDiff.CombineRows(0, row, oldRow), nil
}
