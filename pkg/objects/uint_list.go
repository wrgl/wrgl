// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objects

import (
	"encoding/binary"
	"fmt"
	"io"
)

const maxUint32 = 1 << 32

// UintListEncoder encodes string slice. Max bytes size for each string is 65536 bytes
type UintListEncoder struct {
	buf []byte
}

func NewUintListEncoder() *UintListEncoder {
	return &UintListEncoder{
		buf: make([]byte, 0, 256),
	}
}

func (e *UintListEncoder) Encode(sl []uint32) []byte {
	l := uint32(len(sl))
	bufLen := 4 * int(l+1)
	if bufLen > cap(e.buf) {
		e.buf = make([]byte, bufLen)
	} else {
		e.buf = e.buf[:bufLen]
	}
	if len(sl) > maxUint32 {
		panic(fmt.Errorf("slice length is too long (%d > 4294967296)", len(sl)))
	}
	binary.BigEndian.PutUint32(e.buf, l)
	for i, u := range sl {
		binary.BigEndian.PutUint32(e.buf[(i+1)*4:], u)
	}
	return e.buf
}

// UintListDecoder decodes string slice.
type UintListDecoder struct {
	sl  []uint32
	buf []byte
	pos int64
}

func NewUintListDecoder(reuseRecords bool) *UintListDecoder {
	d := &UintListDecoder{
		buf: make([]byte, 4),
	}
	if reuseRecords {
		d.sl = make([]uint32, 0, 256)
	}
	return d
}

func (d *UintListDecoder) makeUintSlice(n uint32) []uint32 {
	if d.sl == nil {
		return make([]uint32, 0, n)
	}
	if n > uint32(cap(d.sl)) {
		d.sl = make([]uint32, n)
	}
	return d.sl[:0]
}

func (d *UintListDecoder) Decode(b []byte) []uint32 {
	n := binary.BigEndian.Uint32(b)
	sl := d.makeUintSlice(n)
	var i uint32
	for i = 0; i < n; i++ {
		sl = append(sl, binary.BigEndian.Uint32(b[(i+1)*4:]))
	}
	return sl
}

func (d *UintListDecoder) readUint32(r io.Reader) (uint32, error) {
	n, err := r.Read(d.buf)
	if err != nil {
		return 0, err
	}
	d.pos += int64(n)
	return binary.BigEndian.Uint32(d.buf), nil
}

func (d *UintListDecoder) Read(r io.Reader) (int64, []uint32, error) {
	d.pos = 0
	n, err := d.readUint32(r)
	if err != nil {
		return d.pos, nil, err
	}
	sl := d.makeUintSlice(n)
	var i uint32
	for i = 0; i < n; i++ {
		u, err := d.readUint32(r)
		if err != nil {
			return d.pos, nil, err
		}
		sl = append(sl, u)
	}
	return d.pos, sl, nil
}
