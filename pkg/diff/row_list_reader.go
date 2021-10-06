// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"fmt"
	"io"

	"github.com/wrgl/wrgl/pkg/objects"
)

type RowListReader struct {
	off  int
	rows []uint32
	buf  *BlockBuffer
}

func NewRowListReader(db objects.Store, tbl *objects.Table) (*RowListReader, error) {
	buf, err := BlockBufferWithSingleStore(db, []*objects.Table{tbl})
	if err != nil {
		return nil, err
	}
	return &RowListReader{
		buf: buf,
	}, nil
}

func (r *RowListReader) Add(row uint32) {
	r.rows = append(r.rows, row)
}

func (r *RowListReader) Len() int {
	return len(r.rows)
}

func RowToBlockAndOffset(row uint32) (uint32, byte) {
	blk := row / objects.BlockSize
	off := byte(row - blk*objects.BlockSize)
	return blk, off
}

func (r *RowListReader) Read() ([]string, error) {
	if r.off >= r.Len() {
		return nil, io.EOF
	}
	blkOff, rowOff := RowToBlockAndOffset(r.rows[r.off])
	row, err := r.buf.GetRow(0, blkOff, rowOff)
	if err != nil {
		return nil, err
	}
	r.off++
	return row, nil
}

func (r *RowListReader) Seek(offset int, whence int) (int, error) {
	switch whence {
	default:
		return 0, fmt.Errorf("Seek: invalid whence")
	case io.SeekStart:
		break
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += r.Len()
	}
	if offset < 0 {
		return 0, fmt.Errorf("Seek: invalid offset")
	}
	r.off = offset
	return offset, nil
}
