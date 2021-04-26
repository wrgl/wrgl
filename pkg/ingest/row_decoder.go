package ingest

import "encoding/csv"

type decBuffer struct {
	data []byte
	off  int
}

func (b *decBuffer) Read(d []byte) (n int, err error) {
	n = len(b.data) - b.off
	if n > len(d) {
		n = len(d)
	}
	copy(d, b.data[b.off:b.off+n])
	b.off += n
	return n, nil
}

func (b *decBuffer) Set(d []byte) {
	b.data = d
	b.off = 0
}

type RowDecoder struct {
	r *csv.Reader
	b *decBuffer
}

func NewRowDecoder() *RowDecoder {
	dec := new(RowDecoder)
	dec.b = new(decBuffer)
	dec.r = csv.NewReader(dec.b)
	dec.r.FieldsPerRecord = -1
	return dec
}

func (d *RowDecoder) Decode(b []byte) (result []string, err error) {
	d.b.Set(b)
	return d.r.Read()
}

func DecodeRow(b []byte) (result []string, err error) {
	dec := NewRowDecoder()
	return dec.Decode(b)
}
