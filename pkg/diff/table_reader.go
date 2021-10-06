// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"fmt"
	"io"

	"github.com/wrgl/wrgl/pkg/objects"
)

type RowReader interface {
	Read() ([]string, error)
	Seek(offset int, whence int) (int, error)
	Len() int
}

type tableReader struct {
	off int
	tbl *objects.Table
	buf *BlockBuffer
}

func NewTableReader(db objects.Store, tbl *objects.Table) (RowReader, error) {
	buf, err := BlockBufferWithSingleStore(db, []*objects.Table{tbl})
	if err != nil {
		return nil, err
	}
	return &tableReader{
		buf: buf,
		tbl: tbl,
	}, nil
}

func (r *tableReader) Len() int {
	return int(r.tbl.RowsCount)
}

func (r *tableReader) Read() ([]string, error) {
	if r.off >= int(r.tbl.RowsCount) {
		return nil, io.EOF
	}
	blkOffset := r.off / objects.BlockSize
	rowOffset := byte(r.off - blkOffset*objects.BlockSize)
	row, err := r.buf.GetRow(0, uint32(blkOffset), rowOffset)
	if err != nil {
		return nil, err
	}
	r.off++
	return row, nil
}

func (r *tableReader) Seek(offset int, whence int) (int, error) {
	switch whence {
	default:
		return 0, fmt.Errorf("Seek: invalid whence")
	case io.SeekStart:
		break
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += int(r.tbl.RowsCount)
	}
	if offset < 0 {
		return 0, fmt.Errorf("Seek: invalid offset")
	}
	r.off = offset
	return offset, nil
}
