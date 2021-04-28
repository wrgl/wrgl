package encoding

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

type Table struct {
	Columns []string
	PK      []uint32
	Rows    [][32]byte
}

type TableWriter struct {
	w io.Writer
}

func NewTableWriter(w io.Writer) *TableWriter {
	return &TableWriter{
		w: w,
	}
}

func (e *TableWriter) writeLine(label string, b []byte) (err error) {
	_, err = e.w.Write(append([]byte(label), ' '))
	if err != nil {
		return
	}
	_, err = e.w.Write(b)
	if err != nil {
		return
	}
	_, err = e.w.Write([]byte("\n"))
	return
}

func (w *TableWriter) Write(t *Table) (err error) {
	err = w.writeLine("columns", NewStrListEncoder().Encode(t.Columns))
	if err != nil {
		return
	}
	err = w.writeLine("pk", NewUintListEncoder().Encode(t.PK))
	if err != nil {
		return
	}
	rowsCount := make([]byte, 4)
	binary.BigEndian.PutUint32(rowsCount, uint32(len(t.Rows)))
	err = w.writeLine("rows", rowsCount)
	if err != nil {
		return
	}
	for _, b := range t.Rows {
		_, err = w.w.Write(b[:])
		if err != nil {
			return
		}
	}
	return nil
}

type TableReader struct {
	r    io.Reader
	pos  int
	line int
}

func NewTableReader(r io.Reader) *TableReader {
	return &TableReader{
		r: r,
	}
}

func (r *TableReader) parseError(format string, a ...interface{}) error {
	return fmt.Errorf("parse error at pos=%d line=%d: %s", r.pos, r.line, fmt.Sprintf(format, a...))
}

func (r *TableReader) consumeStr(s string) error {
	b := make([]byte, len(s))
	n, err := r.r.Read(b)
	if err != nil {
		return err
	}
	r.pos += n
	r.line += strings.Count(s, "\n")
	if string(b) != s {
		return r.parseError("expected string %q, received %q", s, string(b))
	}
	r.pos += len(s)
	return nil
}

func (r *TableReader) readUint32() (uint32, error) {
	b := make([]byte, 4)
	n, err := r.r.Read(b)
	if err != nil {
		return 0, err
	}
	r.pos += n
	return binary.BigEndian.Uint32(b), nil
}

func (r *TableReader) Read() (t *Table, err error) {
	err = r.consumeStr("columns ")
	if err != nil {
		return
	}
	n, columns, err := NewStrListDecoder(false).Read(r.r)
	if err != nil {
		return
	}
	r.pos += n
	err = r.consumeStr("\npk ")
	if err != nil {
		return
	}
	n, pk, err := NewUintListDecoder(false).Read(r.r)
	if err != nil {
		return
	}
	r.pos += n
	err = r.consumeStr("\nrows ")
	if err != nil {
		return
	}
	rowsCount, err := r.readUint32()
	if err != nil {
		return
	}
	err = r.consumeStr("\n")
	if err != nil {
		return
	}
	rows := make([][32]byte, rowsCount)
	for i, b := range rows {
		n, err := r.r.Read(b[:])
		if err != nil {
			return nil, err
		}
		r.pos += n
		rows[i] = b
	}
	t = &Table{
		Columns: columns,
		PK:      pk,
		Rows:    rows,
	}
	return
}
