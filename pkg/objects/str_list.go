// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objects

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

// StrListEncoder encodes string slice. Max bytes size for each string is 65536 bytes
type StrListEncoder struct {
	buf          []byte
	reuseRecords bool
}

func NewStrListEncoder(reuseRecords bool) *StrListEncoder {
	return &StrListEncoder{
		buf:          make([]byte, 0, 256),
		reuseRecords: reuseRecords,
	}
}

func (e *StrListEncoder) Encode(sl []string) []byte {
	bufLen := 4
	for _, s := range sl {
		bufLen += len(s) + 2
	}
	if bufLen > cap(e.buf) {
		e.buf = make([]byte, bufLen)
	} else {
		e.buf = e.buf[:bufLen]
	}
	binary.BigEndian.PutUint32(e.buf, uint32(len(sl)))
	var offset uint16 = 4
	for _, s := range sl {
		l := uint16(len(s))
		binary.BigEndian.PutUint16(e.buf[offset:], l)
		offset += 2
		copy(e.buf[offset:], s)
		offset += l
	}
	b := e.buf
	if !e.reuseRecords {
		b = make([]byte, len(e.buf))
		copy(b, e.buf)
	}
	return b
}

// StrListDecoder decodes string slice.
type StrListDecoder struct {
	strs         []string
	buf          []byte
	reuseRecords bool
	pos          int
}

func NewStrListDecoder(reuseRecords bool) *StrListDecoder {
	d := &StrListDecoder{
		buf:          make([]byte, 4),
		reuseRecords: reuseRecords,
	}
	if reuseRecords {
		d.strs = make([]string, 0, 256)
	}
	return d
}

func (d *StrListDecoder) strSlice(n uint32) []string {
	if d.strs != nil {
		if n > uint32(cap(d.strs)) {
			d.strs = make([]string, 0, n)
		}
		return d.strs[:0]
	}
	return make([]string, 0, n)
}

func (d *StrListDecoder) Decode(b []byte) []string {
	count := binary.BigEndian.Uint32(b)
	sl := d.strSlice(count)
	var offset uint16 = 4
	var i uint32
	for i = 0; i < count; i++ {
		l := binary.BigEndian.Uint16(b[offset:])
		offset += 2
		if l == 0 {
			sl = append(sl, "")
			continue
		}
		d.ensureBufSize(int(l))
		copy(d.buf[:l], b[offset:])
		offset += l
		sl = append(sl, string(d.buf[:l]))
	}
	return sl
}

func ValidateStrListBytes(b []byte) (int, error) {
	count := int(binary.BigEndian.Uint32(b))
	offset := 4
	n := len(b)
	for i := 0; i < count; i++ {
		l := binary.BigEndian.Uint16(b[offset:])
		offset += 2 + int(l)
		if offset > n {
			return 0, fmt.Errorf("invalid strList")
		}
	}
	return offset, nil
}

func (d *StrListDecoder) ensureBufSize(n int) {
	for n > cap(d.buf) {
		b := make([]byte, cap(d.buf)*2)
		copy(b, d.buf)
		d.buf = b
	}
}

func (d *StrListDecoder) readUint16(r io.Reader) (uint16, error) {
	d.buf[0], d.buf[1] = 0, 0
	b := d.buf[:2]
	n, err := io.ReadFull(r, b)
	d.pos += n
	return binary.BigEndian.Uint16(b), err
}

func (d *StrListDecoder) readUint32(r io.Reader) (uint32, error) {
	b := d.buf[:4]
	n, err := io.ReadFull(r, b)
	if err != nil {
		return 0, err
	}
	d.pos += n
	return binary.BigEndian.Uint32(b), nil
}

func (d *StrListDecoder) Read(r io.Reader) (int64, []string, error) {
	d.pos = 0
	count, err := d.readUint32(r)
	if err != nil {
		return 0, nil, err
	}
	sl := d.strSlice(count)
	var i uint32
	for i = 0; i < count; i++ {
		l, err := d.readUint16(r)
		if err != nil {
			return 0, nil, err
		}
		if l == 0 {
			sl = append(sl, "")
			continue
		}
		d.ensureBufSize(int(l))
		n, err := io.ReadFull(r, d.buf[:l])
		d.pos += n
		sl = append(sl, string(d.buf[:n]))
		if err == io.EOF && i == count-1 {
			break
		}
		if err != nil {
			return 0, nil, err
		}
	}
	return int64(d.pos), sl, nil
}

func (d *StrListDecoder) ReadBytes(r io.Reader) (n int, b []byte, err error) {
	_, err = io.ReadFull(r, d.buf[:4])
	if err != nil {
		return
	}
	count := binary.BigEndian.Uint32(d.buf)
	n = 4
	var i uint32
	var m int
	for i = 0; i < count; i++ {
		d.ensureBufSize(n + 2)
		_, err = io.ReadFull(r, d.buf[n:n+2])
		if err != nil {
			return
		}
		l := binary.BigEndian.Uint16(d.buf[n:])
		n += 2
		d.ensureBufSize(n + int(l))
		m, err = io.ReadFull(r, d.buf[n:n+int(l)])
		n += m
		if err == io.EOF && i == count-1 {
			break
		}
		if err != nil {
			return
		}
	}
	if !d.reuseRecords {
		b = make([]byte, n)
		copy(b, d.buf[:n])
		return n, b, nil
	}
	return n, d.buf[:n], nil
}

type StrList []byte

func (b StrList) seekColumnOffset(u uint32) (off, n int) {
	var i uint32
	l := len(b)
	c := binary.BigEndian.Uint32(b)
	if u >= c {
		panic(fmt.Errorf("column out of bound: %d >= %d", u, c))
	}
	off = 4
	for i = 0; off < l; i++ {
		n = int(binary.BigEndian.Uint16(b[off : off+2]))
		off += 2
		if i == u {
			return
		}
		off += n
	}
	panic(fmt.Errorf("corrupted strList bytes"))
}

func (b StrList) seekColumn(u uint32) []byte {
	off, n := b.seekColumnOffset(u)
	return b[off : off+n]
}

func (b StrList) ReadColumns(columns []uint32) []string {
	sl := make([]string, len(columns))
	for i, u := range columns {
		sl[i] = string(b.seekColumn(u))
	}
	return sl
}

func StringSliceIsLess(pk []uint32, a, b []string) bool {
	if len(pk) == 0 {
		for i, s := range a {
			if s < b[i] {
				return true
			} else if s > b[i] {
				return false
			}
		}
		return false
	}
	for _, u := range pk {
		if a[u] < b[u] {
			return true
		} else if a[u] > b[u] {
			return false
		}
	}
	return false
}

// LessThan returns true if a is less than b based on given column indices
func (b StrList) LessThan(columns []uint32, c StrList) bool {
	if len(columns) == 0 {
		n := binary.BigEndian.Uint32(b)
		var i uint32
		for i = 0; i < n; i++ {
			if v := bytes.Compare(b.seekColumn(i), c.seekColumn(i)); v == 1 {
				return false
			} else if v == -1 {
				return true
			}
		}
		return false
	}
	for _, u := range columns {
		if v := bytes.Compare(b.seekColumn(u), c.seekColumn(u)); v == 1 {
			return false
		} else if v == -1 {
			return true
		}
	}
	return false
}

// StrListEditor can either remove certain columns from StrList or
// remove everything except certain columns. It is built to minimize
// allocations so given StrList will always be edit in place.
type StrListEditor struct {
	sortedColumns []uint32
	colIndices    []int
	offsets       []int
	lens          []int
}

func NewStrListEditor(columns []uint32) *StrListEditor {
	n := len(columns)
	r := &StrListEditor{
		colIndices:    make([]int, n),
		sortedColumns: make([]uint32, n),
		offsets:       make([]int, n),
		lens:          make([]int, n),
	}
	copy(r.sortedColumns, columns)
	sort.Slice(r.sortedColumns, func(i, j int) bool {
		return r.sortedColumns[i] < r.sortedColumns[j]
	})
	m := map[uint32]int{}
	for i, j := range r.sortedColumns {
		m[j] = i
	}
	for i := range r.colIndices {
		r.colIndices[i] = m[columns[i]]
	}
	return r
}

func (r *StrListEditor) findOffsets(b []byte) (origLen uint32) {
	var j uint32
	l := len(b)
	c := binary.BigEndian.Uint32(b)
	off := 4
	var n int
mainLoop:
	for i, u := range r.sortedColumns {
		if u >= c {
			panic(fmt.Errorf("column out of bound: %d >= %d", u, c))
		}
		for off < l {
			n = int(binary.BigEndian.Uint16(b[off:]))
			if j == u {
				r.offsets[i] = off
				r.lens[i] = n + 2
			}
			off += 2 + n
			j++
			if j-1 == u {
				continue mainLoop
			}
		}
		panic(fmt.Errorf("corrupted strList bytes"))
	}
	return c
}

func (r *StrListEditor) RemoveFrom(b []byte) []byte {
	l := r.findOffsets(b)
	binary.BigEndian.PutUint32(b, l-uint32(len(r.offsets)))
	for i := len(r.offsets) - 1; i >= 0; i-- {
		b = append(b[:r.offsets[i]], b[r.offsets[i]+r.lens[i]:]...)
	}
	return b
}

func (r *StrListEditor) ensureLength(b []byte, n int) []byte {
	if n > cap(b) {
		c := make([]byte, n)
		copy(c, b)
		b = c
	} else {
		b = b[:n]
	}
	return b
}

func (r *StrListEditor) PickFrom(dst, src []byte) []byte {
	r.findOffsets(src)
	total := 0
	for _, n := range r.lens {
		total += n
	}
	dst = r.ensureLength(dst, 4+total)
	binary.BigEndian.PutUint32(dst, uint32(len(r.sortedColumns)))
	off := 4
	for _, i := range r.colIndices {
		copy(dst[off:], src[r.offsets[i]:r.offsets[i]+r.lens[i]])
		off += r.lens[i]
	}
	return dst
}
