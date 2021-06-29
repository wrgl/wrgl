// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"bytes"
	"container/list"
	"fmt"
	"io"

	"github.com/wrgl/core/pkg/kv"
	kvcommon "github.com/wrgl/core/pkg/kv/common"
	"github.com/wrgl/core/pkg/mem"
	"github.com/wrgl/core/pkg/objects"
)

func getBufferSize() (uint64, error) {
	total, err := mem.GetTotalMem()
	if err != nil {
		return 0, err
	}
	avail, err := mem.GetAvailMem()
	if err != nil {
		return 0, err
	}
	size := avail
	if size < total/8 {
		size = total / 8
	}
	return size / 2, nil
}

type blockEl struct {
	Table  byte
	Offset uint32
	Size   uint64
	Block  [][]string
}

type blockBuffer struct {
	db            []kvcommon.DB
	tbl           []*objects.Table
	buf           *list.List
	maxSize, size uint64
}

func newBlockBuffer(db1, db2 kvcommon.DB, tbl1, tbl2 *objects.Table) (*blockBuffer, error) {
	maxSize, err := getBufferSize()
	if err != nil {
		return nil, err
	}
	return &blockBuffer{
		db:      []kvcommon.DB{db1, db2},
		tbl:     []*objects.Table{tbl1, tbl2},
		buf:     list.New(),
		maxSize: maxSize,
	}, nil
}

func (buf *blockBuffer) addBlock(table byte, offset uint32) ([][]string, error) {
	if buf.size >= buf.maxSize {
		buf.size -= buf.buf.Remove(buf.buf.Back()).(*blockEl).Size
	}
	b, err := kv.GetBlock(buf.db[table], buf.tbl[table].Blocks[offset])
	if err != nil {
		return nil, err
	}
	_, blk, err := objects.ReadBlockFrom(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	n := len(blk)
	m := len(blk[0])
	size := uint64(24 + 24*n + len(b) - n*(4+m*2))
	buf.buf.PushFront(&blockEl{
		Table:  table,
		Offset: offset,
		Block:  blk,
		Size:   size,
	})
	buf.size += size
	return blk, nil
}

func (buf *blockBuffer) getBlock(table byte, offset uint32) ([][]string, error) {
	el := buf.buf.Front()
	for el != nil {
		be := el.Value.(*blockEl)
		if be.Table == table && be.Offset == offset {
			buf.buf.MoveToFront(el)
			return be.Block, nil
		}
		el = el.Next()
	}
	return buf.addBlock(table, offset)
}

func (buf *blockBuffer) getRow(table byte, offset uint32, rowOffset byte) ([]string, error) {
	blk, err := buf.getBlock(table, offset)
	if err != nil {
		return nil, err
	}
	return blk[rowOffset], nil
}

type RowChangeReader struct {
	ColDiff  *objects.ColDiff
	rowDiffs []*objects.Diff
	off      int
	buf      *blockBuffer
}

func NewRowChangeReader(db1, db2 kvcommon.DB, tbl1, tbl2 *objects.Table, colDiff *objects.ColDiff) (*RowChangeReader, error) {
	buf, err := newBlockBuffer(db1, db2, tbl1, tbl2)
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

func (r *RowChangeReader) NumRows() int {
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
	row, err := r.buf.getRow(1, d.Block, d.Row)
	if err != nil {
		return nil, err
	}
	oldRow, err := r.buf.getRow(2, d.OldBlock, d.OldRow)
	if err != nil {
		return nil, err
	}
	return r.ColDiff.CombineRows(0, row, oldRow), nil
}
