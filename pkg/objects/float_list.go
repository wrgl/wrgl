// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objects

import (
	"encoding/binary"
	"io"
	"math"
)

// FloatListEncoder encodes string slice. Max bytes size for each string is 65536 bytes
type FloatListEncoder struct {
	buf []byte
}

func NewFloatListEncoder() *FloatListEncoder {
	return &FloatListEncoder{
		buf: make([]byte, 0, 256),
	}
}

func (e *FloatListEncoder) Encode(sl []float64) []byte {
	l := uint32(len(sl))
	bufLen := 8*int(l) + 4
	if bufLen > cap(e.buf) {
		e.buf = make([]byte, bufLen)
	} else {
		e.buf = e.buf[:bufLen]
	}
	binary.BigEndian.PutUint32(e.buf, l)
	for i, f := range sl {
		bits := math.Float64bits(f)
		binary.BigEndian.PutUint64(e.buf[4+i*8:], bits)
	}
	return e.buf
}

// FloatListDecoder decodes string slice.
type FloatListDecoder struct {
	sl  []float64
	buf []byte
	pos int64
}

func NewFloatListDecoder(reuseRecords bool) *FloatListDecoder {
	d := &FloatListDecoder{
		buf: make([]byte, 8),
	}
	if reuseRecords {
		d.sl = make([]float64, 0, 256)
	}
	return d
}

func (d *FloatListDecoder) makeFloatSlice(n uint32) []float64 {
	if d.sl == nil {
		return make([]float64, 0, n)
	}
	if n > uint32(cap(d.sl)) {
		d.sl = make([]float64, n)
	}
	return d.sl[:0]
}

func (d *FloatListDecoder) Decode(b []byte) []float64 {
	n := binary.BigEndian.Uint32(b)
	sl := d.makeFloatSlice(n)
	var i uint32
	for i = 0; i < n; i++ {
		bits := binary.BigEndian.Uint64(b[4+8*i:])
		sl = append(sl, math.Float64frombits(bits))
	}
	return sl
}

func (d *FloatListDecoder) readUint32(r io.Reader) (uint32, error) {
	n, err := r.Read(d.buf[:4])
	if err != nil {
		return 0, err
	}
	d.pos += int64(n)
	return binary.BigEndian.Uint32(d.buf), nil
}

func (d *FloatListDecoder) readFloat64(r io.Reader) (float64, error) {
	n, err := r.Read(d.buf)
	if err != nil {
		return 0, err
	}
	d.pos += int64(n)
	bits := binary.BigEndian.Uint64(d.buf)
	return math.Float64frombits(bits), nil
}

func (d *FloatListDecoder) Read(r io.Reader) (int64, []float64, error) {
	d.pos = 0
	n, err := d.readUint32(r)
	if err != nil {
		return d.pos, nil, err
	}
	sl := d.makeFloatSlice(n)
	var i uint32
	for i = 0; i < n; i++ {
		f, err := d.readFloat64(r)
		if err != nil {
			return d.pos, nil, err
		}
		sl = append(sl, f)
	}
	return d.pos, sl, nil
}
