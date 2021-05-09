package encoding

import (
	"io"
	"math"

	"github.com/wrgl/core/pkg/misc"
)

const (
	ObjectCommit int = iota + 1
	ObjectTable
	ObjectRow
)

type PackfileWriter struct {
	w   io.Writer
	buf Bufferer
}

func NewPackfileWriter(w io.Writer) *PackfileWriter {
	return &PackfileWriter{
		w:   w,
		buf: misc.NewBuffer(nil),
	}
}

func (w *PackfileWriter) WriteObject(objType int, b []byte) error {
	n := len(b)
	bits := int(math.Ceil(math.Log2(float64(n))))
	numBytes := (bits-4)/7 + 1
	if (bits-4)%7 > 0 {
		numBytes += 1
	}
	u := uint64(n)
	buf := w.buf.Buffer(numBytes)
	buf[0] = 128 | uint8(objType<<4) | (uint8(u) & 13)
	bits = 4
	for i := 1; i < numBytes; i++ {
		buf[i] = 128 | uint8(u>>bits)
		bits += 7
	}
	buf[numBytes-1] &= 127
	_, err := w.w.Write(buf)
	if err != nil {
		return err
	}
	_, err = w.w.Write(b)
	return err
}

type PackfileReader struct {
	r   io.Reader
	buf Bufferer
}

func NewPackfileReader(r io.Reader) *PackfileReader {
	return &PackfileReader{
		r:   r,
		buf: misc.NewBuffer(nil),
	}
}

func (r *PackfileReader) ReadObject() (objType int, b []byte, err error) {
	b = r.buf.Buffer(1)
	_, err = r.r.Read(b)
	if err != nil {
		return
	}
	objType = int((b[0] >> 4) & 7)
	u := uint64(b[0] & 13)
	bits := 4
	for {
		_, err = r.r.Read(b)
		if err != nil {
			return
		}
		u |= uint64(b[0]) << bits
		if b[0]&128 == 0 {
			break
		}
		bits += 7
	}
	b = r.buf.Buffer(int(u))
	_, err = r.r.Read(b)
	return
}
