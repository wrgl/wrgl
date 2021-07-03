// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"fmt"
	"io"

	"github.com/wrgl/core/pkg/objects"
)

type RowReader struct {
	off int
	tbl *objects.Table
	buf *BlockBuffer
}

func NewRowReader(db objects.Store, tbl *objects.Table) (*RowReader, error) {
	buf, err := BlockBufferWithSingleStore(db, []*objects.Table{tbl})
	if err != nil {
		return nil, err
	}
	return &RowReader{
		buf: buf,
		tbl: tbl,
	}, nil
}

func (r *RowReader) Read() ([]string, error) {
	if r.off >= int(r.tbl.RowsCount) {
		return nil, io.EOF
	}
	blkOffset := r.off / 255
	rowOffset := byte(r.off - blkOffset*255)
	row, err := r.buf.GetRow(0, uint32(blkOffset), rowOffset)
	if err != nil {
		return nil, err
	}
	r.off++
	return row, nil
}

func (r *RowReader) Seek(offset int, whence int) (int, error) {
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
