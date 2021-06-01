package objects

import (
	"fmt"
	"io"

	"github.com/wrgl/core/pkg/misc"
)

type DiffType byte

const (
	DTUnspecified DiffType = iota
	DTColumnChange
	DTRow
)

type Diff struct {
	Type DiffType

	// DTColumnChange fields
	ColDiff *ColDiff

	// DTRow* fields
	PK     []byte
	Sum    []byte
	OldSum []byte
}

type DiffWriter struct {
	w   io.Writer
	buf *misc.Buffer
}

func NewDiffWriter(w io.Writer) *DiffWriter {
	return &DiffWriter{
		w:   w,
		buf: misc.NewBuffer(nil),
	}
}

func (w *DiffWriter) Write(d *Diff) (err error) {
	b := w.buf.Buffer(2)
	b[0] = byte(d.Type)
	b[1] = ' '
	_, err = w.w.Write(b)
	if err != nil {
		return
	}
	switch d.Type {
	case DTColumnChange:
		b, err = EncodeColDiff(d.ColDiff)
		if err != nil {
			return
		}
	case DTRow:
		b = w.buf.Buffer(48)
		copy(b, d.PK)
		if d.OldSum == nil {
			for i := 16; i < 32; i++ {
				b[i] = 0
			}
		} else {
			copy(b[16:], d.OldSum)
		}
		if d.Sum == nil {
			for i := 32; i < 48; i++ {
				b[i] = 0
			}
		} else {
			copy(b[32:], d.Sum)
		}
	default:
		err = fmt.Errorf("unhandled diff type %v", d.Type)
		return
	}
	_, err = w.w.Write(b)
	if err != nil {
		return
	}
	b = w.buf.Buffer(1)
	b[0] = '\n'
	_, err = w.w.Write(b)
	return
}

type DiffReader struct {
	r   io.Reader
	off int64
}

func NewDiffReader(r io.Reader) *DiffReader {
	return &DiffReader{
		r: r,
	}
}

func (r *DiffReader) readSum(b []byte) []byte {
	for i := 0; i < 16; i++ {
		if b[i] != 0 {
			return b[:16]
		}
	}
	return nil
}

func (r *DiffReader) Read() (d *Diff, err error) {
	b := make([]byte, 2)
	n, err := r.r.Read(b)
	if err != nil {
		return
	}
	r.off += int64(n)
	d = &Diff{
		Type: DiffType(b[0]),
	}
	if b[1] != ' ' {
		err = fmt.Errorf("parse error at pos=%d: expected space, found %q", r.off, b[1])
		return
	}
	switch d.Type {
	case DTColumnChange:
		colDiffR := NewColDiffReader(r.r)
		n, colDiff, err := colDiffR.Read()
		if err != nil {
			return nil, err
		}
		r.off += int64(n)
		d.ColDiff = colDiff
	case DTRow:
		b = make([]byte, 48)
		n, err = r.r.Read(b)
		if err != nil {
			return
		}
		r.off += int64(n)
		d.PK = b[:16]
		d.OldSum = r.readSum(b[16:])
		d.Sum = r.readSum(b[32:])
	default:
		err = fmt.Errorf("unhandled diff type %v", d.Type)
		return
	}
	b = make([]byte, 1)
	n, err = r.r.Read(b)
	if err != nil {
		return
	}
	r.off += int64(n)
	if b[0] != '\n' {
		err = fmt.Errorf("parse error at pos=%d: expected line feed, found %q", r.off, b[1])
	}
	return
}
