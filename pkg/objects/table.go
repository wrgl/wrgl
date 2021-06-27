// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type Table struct {
	Columns   []string
	PK        []uint32
	RowsCount uint32
	Blocks    [][]byte
}

type WriteSeekerAt interface {
	io.WriteSeeker
	io.WriterAt
}

type TableWriter struct {
	w              WriteSeekerAt
	off            int64
	blocksStartOff int64
}

func NewTableWriter(w WriteSeekerAt) *TableWriter {
	return &TableWriter{
		w: w,
	}
}

func (w *TableWriter) writeLine(label string, b []byte) error {
	n, err := writeLine(w.w, label, b)
	if err != nil {
		return err
	}
	w.off += int64(n)
	return nil
}

func (w *TableWriter) WriteMeta(columns []string, pk []uint32, rowsCount uint32) (err error) {
	err = w.writeLine("columns", NewStrListEncoder(true).Encode(columns))
	if err != nil {
		return
	}
	err = w.writeLine("pk", NewUintListEncoder().Encode(pk))
	if err != nil {
		return
	}
	// leave blank rows count and blocks count for now
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(rowsCount))
	err = w.writeLine("rows", b)
	if err != nil {
		return
	}
	w.blocksStartOff = w.off
	return
}

func (w *TableWriter) writeBlock(b []byte) (err error) {
	var n int
	n, err = w.w.Write(b)
	if err != nil {
		return
	}
	w.off += int64(n)
	return
}

func (r *TableWriter) blockPos(offset int) int64 {
	return int64(offset)*16 + r.blocksStartOff
}

func (w *TableWriter) WriteBlockAt(b []byte, offset int) (err error) {
	_, err = w.w.WriteAt(b, w.blockPos(offset))
	if err != nil {
		return
	}
	return
}

func (w *TableWriter) WriteTable(t *Table) (err error) {
	err = w.WriteMeta(t.Columns, t.PK, t.RowsCount)
	if err != nil {
		return
	}
	for _, b := range t.Blocks {
		err = w.writeBlock(b)
		if err != nil {
			return
		}
	}
	return nil
}

type TableReader struct {
	r              io.ReadSeekCloser
	off            int64
	blocksStartOff int64
	rowsCount      uint32
	blocksCount    uint32
	Columns        []string
	PK             []uint32
}

func NewTableReader(r io.ReadSeekCloser) (*TableReader, error) {
	reader := &TableReader{
		r: r,
	}
	err := reader.readMeta()
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (r *TableReader) parseError(format string, a ...interface{}) error {
	return fmt.Errorf("parse error at pos=%d: %s", r.off, fmt.Sprintf(format, a...))
}

func (r *TableReader) consumeStr(s string) error {
	b := make([]byte, len(s))
	n, err := r.r.Read(b)
	if err != nil {
		return err
	}
	r.off += int64(n)
	if string(b) != s {
		return r.parseError("expected string %q, received %q", s, string(b))
	}
	return nil
}

func (r *TableReader) readUint32() (uint32, error) {
	b := make([]byte, 4)
	n, err := r.r.Read(b)
	if err != nil {
		return 0, err
	}
	r.off += int64(n)
	return binary.BigEndian.Uint32(b), nil
}

func (r *TableReader) readMeta() (err error) {
	err = r.consumeStr("columns ")
	if err != nil {
		return
	}
	var n int
	n, r.Columns, err = NewStrListDecoder(false).Read(r.r)
	if err != nil {
		return
	}
	r.off += int64(n)
	err = r.consumeStr("\npk ")
	if err != nil {
		return
	}
	n, r.PK, err = NewUintListDecoder(false).Read(r.r)
	if err != nil {
		return
	}
	r.off += int64(n)
	err = r.consumeStr("\nrows ")
	if err != nil {
		return
	}
	r.rowsCount, err = r.readUint32()
	if err != nil {
		return
	}
	err = r.consumeStr("\n")
	r.blocksStartOff = r.off
	r.blocksCount = uint32(math.Ceil(float64(r.rowsCount) / float64(128)))
	return
}

func (r *TableReader) RowsCount() int {
	return int(r.rowsCount)
}

func (r *TableReader) ReadBlock() (b []byte, err error) {
	b = make([]byte, 16)
	n, err := r.r.Read(b)
	if err != nil {
		return nil, err
	}
	r.off += int64(n)
	return
}

func (r *TableReader) blockPos(offset int) int64 {
	return int64(offset)*16 + r.blocksStartOff
}

func (r *TableReader) SeekBlock(offset int, whence int) (int, error) {
	switch whence {
	default:
		return 0, fmt.Errorf("seek: invalid whence")
	case io.SeekStart:
		break
	case io.SeekCurrent:
		offset += int((r.off - r.blocksStartOff) / 16)
	case io.SeekEnd:
		offset += int(r.blocksCount)
	}
	if offset < 0 {
		return 0, fmt.Errorf("seek: invalid offset")
	}
	var err error
	r.off, err = r.r.Seek(r.blockPos(offset), io.SeekStart)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (r *TableReader) ReadBlockAt(offset int) (b []byte, err error) {
	off := r.off
	_, err = r.SeekBlock(offset, io.SeekStart)
	if err != nil {
		return
	}
	b, err = r.ReadBlock()
	if err != nil {
		return
	}
	r.off, err = r.r.Seek(off, io.SeekStart)
	return
}

func (r *TableReader) ReadTable() (t *Table, err error) {
	blocks := make([][]byte, r.blocksCount)
	for i, b := range blocks {
		b, err = r.ReadBlock()
		blocks[i] = b
	}
	t = &Table{
		Columns:   r.Columns,
		PK:        r.PK,
		Blocks:    blocks,
		RowsCount: r.rowsCount,
	}
	return
}

func (r *TableReader) Close() error {
	return r.r.Close()
}
