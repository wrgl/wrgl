// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Table struct {
	Columns []string
	PK      []uint32
	Rows    [][]byte
}

type WriteSeekerAt interface {
	io.WriteSeeker
	io.WriterAt
}

type TableWriter struct {
	w            WriteSeekerAt
	off          int64
	rowsStartOff int64
	rowsCount    uint32
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

func (w *TableWriter) WriteMeta(columns []string, pk []uint32) (err error) {
	err = w.writeLine("columns", NewStrListEncoder().Encode(columns))
	if err != nil {
		return
	}
	err = w.writeLine("pk", NewUintListEncoder().Encode(pk))
	if err != nil {
		return
	}
	// leave 10 bytes to write rows count when finish
	w.off, err = w.w.Seek(10, io.SeekCurrent)
	if err != nil {
		return
	}
	w.rowsStartOff = w.off
	return
}

// Flush writes rows count
func (w *TableWriter) Flush() (err error) {
	b := make([]byte, 10)
	copy(b, []byte("rows "))
	binary.BigEndian.PutUint32(b[5:], w.rowsCount)
	b[9] = '\n'
	_, err = w.w.WriteAt(b, w.rowsStartOff-10)
	return
}

func (w *TableWriter) writeRow(b []byte) (err error) {
	var n int
	n, err = w.w.Write(b)
	if err != nil {
		return
	}
	w.off += int64(n)
	return
}

func (r *TableWriter) rowPos(offset int) int64 {
	return int64(offset)*32 + r.rowsStartOff
}

func (w *TableWriter) WriteRowAt(b []byte, offset int) (err error) {
	_, err = w.w.WriteAt(b, w.rowPos(offset))
	if err != nil {
		return
	}
	if uint32(offset) >= w.rowsCount {
		w.rowsCount = uint32(offset + 1)
	}
	return
}

func (w *TableWriter) WriteTable(t *Table) (err error) {
	err = w.WriteMeta(t.Columns, t.PK)
	if err != nil {
		return
	}
	for _, b := range t.Rows {
		err = w.writeRow(b)
		if err != nil {
			return
		}
	}
	w.rowsCount = uint32((w.off - w.rowsStartOff) / 32)
	return w.Flush()
}

type TableReader struct {
	r            io.ReadSeekCloser
	off          int64
	rowsStartOff int64
	rowsCount    uint32
	Columns      []string
	PK           []uint32
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
	r.rowsStartOff = r.off
	return
}

func (r *TableReader) RowsCount() int {
	return int(r.rowsCount)
}

func (r *TableReader) ReadRow() (b []byte, err error) {
	b = make([]byte, 32)
	n, err := r.r.Read(b)
	if err != nil {
		return nil, err
	}
	r.off += int64(n)
	return
}

func (r *TableReader) rowPos(offset int) int64 {
	return int64(offset)*32 + r.rowsStartOff
}

func (r *TableReader) SeekRow(offset int, whence int) (int, error) {
	switch whence {
	default:
		return 0, fmt.Errorf("seek: invalid whence")
	case io.SeekStart:
		break
	case io.SeekCurrent:
		offset += int((r.off - r.rowsStartOff) / 32)
	case io.SeekEnd:
		offset += int(r.rowsCount)
	}
	if offset < 0 {
		return 0, fmt.Errorf("seek: invalid offset")
	}
	var err error
	r.off, err = r.r.Seek(r.rowPos(offset), io.SeekStart)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (r *TableReader) ReadRowAt(offset int) (b []byte, err error) {
	off := r.off
	_, err = r.SeekRow(offset, io.SeekStart)
	if err != nil {
		return
	}
	b, err = r.ReadRow()
	if err != nil {
		return
	}
	r.off, err = r.r.Seek(off, io.SeekStart)
	return
}

func (r *TableReader) ReadTable() (t *Table, err error) {
	rows := make([][]byte, r.rowsCount)
	for i, b := range rows {
		b, err = r.ReadRow()
		rows[i] = b
	}
	t = &Table{
		Columns: r.Columns,
		PK:      r.PK,
		Rows:    rows,
	}
	return
}

func (r *TableReader) Close() error {
	return r.r.Close()
}
