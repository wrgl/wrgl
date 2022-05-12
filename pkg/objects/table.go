// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objects

import (
	"io"
	"math"

	"github.com/wrgl/wrgl/pkg/encoding"
	"github.com/wrgl/wrgl/pkg/encoding/objline"
	"github.com/wrgl/wrgl/pkg/misc"
	"github.com/wrgl/wrgl/pkg/slice"
)

type Table struct {
	Sum          []byte
	Columns      []string
	PK           []uint32
	RowsCount    uint32
	Blocks       [][]byte
	BlockIndices [][]byte
}

func BlocksCount(rowsCount uint32) uint32 {
	return uint32(math.Ceil(float64(rowsCount) / float64(255)))
}

func NewTable(columns []string, pk []uint32) *Table {
	return &Table{
		Columns: columns,
		PK:      pk,
	}
}

func (t *Table) PrimaryKey() []string {
	return slice.IndicesToValues(t.Columns, t.PK)
}

func (t *Table) writeMeta(w io.Writer, columns []string, pk []uint32, rowsCount uint32) (total int64, err error) {
	buf := misc.NewBuffer(nil)
	for _, f := range []fieldEncode{
		{"columns", objline.WriteBytes(NewStrListEncoder(true).Encode(columns))},
		{"pk", objline.WriteBytes(NewUintListEncoder().Encode(pk))},
		{"rows", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteUint32(w, buf, rowsCount)
		}},
	} {
		n, err := objline.WriteField(w, buf, f.label, f.f)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return
}

func (t *Table) WriteTo(w io.Writer) (int64, error) {
	n, err := t.writeMeta(w, t.Columns, t.PK, t.RowsCount)
	if err != nil {
		return 0, err
	}
	total := n
	for _, b := range t.Blocks {
		n, err := w.Write(b)
		if err != nil {
			return total, err
		}
		total += int64(n)
	}
	for _, b := range t.BlockIndices {
		n, err := w.Write(b)
		if err != nil {
			return total, err
		}
		total += int64(n)
	}
	return total, nil
}

func (t *Table) readMeta(r io.Reader) (total int64, err error) {
	parser := encoding.NewParser(r)
	for _, f := range []fieldDecode{
		{"columns", func(p *encoding.Parser) (n int64, err error) {
			n, t.Columns, err = NewStrListDecoder(false).Read(p)
			if err != nil {
				return 0, err
			}
			return n, nil
		}},
		{"pk", func(p *encoding.Parser) (n int64, err error) {
			n, t.PK, err = NewUintListDecoder(false).Read(p)
			if err != nil {
				return 0, err
			}
			return n, nil
		}},
		{"rows", func(p *encoding.Parser) (int64, error) {
			return objline.ReadUint32(p, &t.RowsCount)
		}},
	} {
		n, err := objline.ReadField(parser, f.label, f.f)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return
}

func (t *Table) readBlock(r io.Reader) (int, []byte, error) {
	b := make([]byte, 16)
	n, err := r.Read(b)
	if err != nil {
		return 0, nil, err
	}
	return n, b, nil
}

func (t *Table) ReadFrom(r io.Reader) (int64, error) {
	n, err := t.readMeta(r)
	if err != nil {
		return 0, err
	}
	total := int64(n)
	blocksCount := uint32(math.Ceil(float64(t.RowsCount) / float64(255)))
	t.Blocks = make([][]byte, blocksCount)
	t.BlockIndices = make([][]byte, blocksCount)
	for i := range t.Blocks {
		n, b, err := t.readBlock(r)
		if err != nil {
			return 0, err
		}
		total += int64(n)
		t.Blocks[i] = b
	}
	for i := range t.BlockIndices {
		n, b, err := t.readBlock(r)
		if err != nil {
			return 0, err
		}
		total += int64(n)
		t.BlockIndices[i] = b
	}
	return total, nil
}

func ReadTableFrom(r io.Reader) (int64, *Table, error) {
	t := &Table{}
	n, err := t.ReadFrom(r)
	return n, t, err
}
