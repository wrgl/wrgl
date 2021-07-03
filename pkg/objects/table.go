// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/wrgl/core/pkg/slice"
)

type Table struct {
	Columns   []string
	PK        []uint32
	RowsCount uint32
	Blocks    [][]byte
}

func BlocksCount(rowsCount uint32) uint32 {
	return uint32(math.Ceil(float64(rowsCount) / float64(255)))
}

func NewTable(columns []string, pk []uint32, rowsCount uint32) *Table {
	return &Table{
		Columns:   columns,
		PK:        pk,
		RowsCount: rowsCount,
		Blocks:    make([][]byte, BlocksCount(rowsCount)),
	}
}

func (t *Table) PrimaryKey() []string {
	return slice.IndicesToValues(t.Columns, t.PK)
}

func (t *Table) writeLine(w io.Writer, label string, b []byte) (int64, error) {
	n, err := writeLine(w, label, b)
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

func (t *Table) writeMeta(w io.Writer, columns []string, pk []uint32, rowsCount uint32) (total int64, err error) {
	n, err := t.writeLine(w, "columns", NewStrListEncoder(true).Encode(columns))
	if err != nil {
		return
	}
	total = n
	n, err = t.writeLine(w, "pk", NewUintListEncoder().Encode(pk))
	if err != nil {
		return
	}
	total += n
	// leave blank rows count and blocks count for now
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(rowsCount))
	n, err = t.writeLine(w, "rows", b)
	if err != nil {
		return
	}
	total += n
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
	return total, nil
}

func parseError(off int, format string, a ...interface{}) error {
	return fmt.Errorf("parse error at pos=%d: %s", off, fmt.Sprintf(format, a...))
}

func consumeStrFromReader(r io.Reader, off int, s string) (int, error) {
	b := make([]byte, len(s))
	n, err := r.Read(b)
	if err != nil {
		return 0, err
	}
	if string(b) != s {
		return 0, parseError(off, "expected string %q, received %q", s, string(b))
	}
	return n, nil
}

func readUint32(r io.Reader) (int, uint32, error) {
	b := make([]byte, 4)
	n, err := r.Read(b)
	if err != nil {
		return 0, 0, err
	}
	return n, binary.BigEndian.Uint32(b), nil
}

func (t *Table) readMeta(r io.Reader) (total int, err error) {
	n, err := consumeStrFromReader(r, 0, "columns ")
	if err != nil {
		return
	}
	total = n
	n, t.Columns, err = NewStrListDecoder(false).Read(r)
	if err != nil {
		return
	}
	total += n
	n, err = consumeStrFromReader(r, total, "\npk ")
	if err != nil {
		return
	}
	total += n
	n, t.PK, err = NewUintListDecoder(false).Read(r)
	if err != nil {
		return
	}
	total += n
	n, err = consumeStrFromReader(r, total, "\nrows ")
	if err != nil {
		return
	}
	total += n
	n, t.RowsCount, err = readUint32(r)
	if err != nil {
		return
	}
	total += n
	n, err = consumeStrFromReader(r, total, "\n")
	if err != nil {
		return
	}
	total += n
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
	for i := range t.Blocks {
		n, b, err := t.readBlock(r)
		if err != nil {
			return 0, err
		}
		total += int64(n)
		t.Blocks[i] = b
	}
	return total, nil
}

func ReadTableFrom(r io.Reader) (int64, *Table, error) {
	t := &Table{}
	n, err := t.ReadFrom(r)
	return n, t, err
}
