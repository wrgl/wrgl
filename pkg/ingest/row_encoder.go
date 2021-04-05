package ingest

import "encoding/csv"

type encBuffer struct {
	data []byte
	off  int
}

func (b *encBuffer) Write(d []byte) (n int, err error) {
	b.off = len(b.data)
	b.data = append(b.data, d...)
	return len(d), nil
}

func (b *encBuffer) Bytes() (result []byte) {
	return b.data[b.off:]
}

func (b *encBuffer) Reset() {
	b.data = b.data[:0]
}

type RowEncoder struct {
	b *encBuffer
	w *csv.Writer
}

func NewRowEncoder() *RowEncoder {
	e := new(RowEncoder)
	e.b = new(encBuffer)
	e.w = csv.NewWriter(e.b)
	return e
}

func (e *RowEncoder) Encode(row []string) (result []byte, err error) {
	err = e.w.Write(row)
	if err != nil {
		return
	}
	e.w.Flush()
	err = e.w.Error()
	if err != nil {
		return
	}
	return e.b.Bytes(), nil
}

func (e *RowEncoder) Reset() {
	e.b.Reset()
}
