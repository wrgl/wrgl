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

type TableWriter struct {
	w io.Writer
}

func NewTableWriter(w io.Writer) *TableWriter {
	return &TableWriter{
		w: w,
	}
}

func (w *TableWriter) Write(t *Table) (err error) {
	err = writeLine(w.w, "columns", NewStrListEncoder().Encode(t.Columns))
	if err != nil {
		return
	}
	err = writeLine(w.w, "pk", NewUintListEncoder().Encode(t.PK))
	if err != nil {
		return
	}
	rowsCount := make([]byte, 4)
	binary.BigEndian.PutUint32(rowsCount, uint32(len(t.Rows)))
	err = writeLine(w.w, "rows", rowsCount)
	if err != nil {
		return
	}
	for _, b := range t.Rows {
		_, err = w.w.Write(b)
		if err != nil {
			return
		}
	}
	return nil
}

type TableReader struct {
	r   io.Reader
	pos int
}

func NewTableReader(r io.Reader) *TableReader {
	return &TableReader{
		r: r,
	}
}

func (r *TableReader) parseError(format string, a ...interface{}) error {
	return fmt.Errorf("parse error at pos=%d: %s", r.pos, fmt.Sprintf(format, a...))
}

func (r *TableReader) consumeStr(s string) error {
	b := make([]byte, len(s))
	n, err := r.r.Read(b)
	if err != nil {
		return err
	}
	r.pos += n
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
	rows := make([][]byte, rowsCount)
	for i, b := range rows {
		b = make([]byte, 32)
		n, err := r.r.Read(b)
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
