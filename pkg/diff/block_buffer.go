// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"bytes"
	"compress/gzip"
	"container/list"

	"github.com/wrgl/wrgl/pkg/mem"
	"github.com/wrgl/wrgl/pkg/objects"
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

type BlockBuffer struct {
	db            []objects.Store
	tbl           []*objects.Table
	buf           *list.List
	blkBuf        *bytes.Buffer
	gzr           *gzip.Reader
	maxSize, size uint64
}

func NewBlockBuffer(db []objects.Store, tbl []*objects.Table) (*BlockBuffer, error) {
	maxSize, err := getBufferSize()
	if err != nil {
		return nil, err
	}
	return &BlockBuffer{
		db:      db,
		tbl:     tbl,
		buf:     list.New(),
		gzr:     new(gzip.Reader),
		blkBuf:  bytes.NewBuffer(nil),
		maxSize: maxSize,
	}, nil
}

func BlockBufferWithSingleStore(db objects.Store, tbl []*objects.Table) (*BlockBuffer, error) {
	sl := make([]objects.Store, len(tbl))
	for i := range sl {
		sl[i] = db
	}
	return NewBlockBuffer(sl, tbl)
}

func (buf *BlockBuffer) addBlock(table byte, offset uint32) ([][]string, error) {
	if buf.size >= buf.maxSize {
		buf.size -= buf.buf.Remove(buf.buf.Back()).(*blockEl).Size
	}
	blk, err := objects.GetBlock(buf.db[table], buf.blkBuf, buf.gzr, buf.tbl[table].Blocks[offset])
	if err != nil {
		return nil, err
	}
	n := len(blk)
	size := uint64(24 + 24*n)
	for _, sl := range blk {
		for _, s := range sl {
			size += uint64(len(s))
		}
	}
	buf.buf.PushFront(&blockEl{
		Table:  table,
		Offset: offset,
		Block:  blk,
		Size:   size,
	})
	buf.size += size
	return blk, nil
}

func (buf *BlockBuffer) getBlock(table byte, offset uint32) ([][]string, error) {
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

func (buf *BlockBuffer) GetRow(table byte, offset uint32, rowOffset byte) ([]string, error) {
	blk, err := buf.getBlock(table, offset)
	if err != nil {
		return nil, err
	}
	return blk[rowOffset], nil
}
