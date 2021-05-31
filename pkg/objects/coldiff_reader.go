// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/wrgl/core/pkg/misc"
)

type ColDiffReader struct {
	r       io.Reader
	off     int
	buf     *misc.Buffer
	strDec  *StrListDecoder
	uintDec *UintListDecoder
}

func NewColDiffReader(r io.Reader) *ColDiffReader {
	return &ColDiffReader{
		r:       r,
		buf:     misc.NewBuffer(nil),
		strDec:  NewStrListDecoder(false),
		uintDec: NewUintListDecoder(false),
	}
}

func (r *ColDiffReader) read(b []byte) error {
	n, err := r.r.Read(b)
	if err != nil {
		return err
	}
	r.off += n
	return nil
}

func (r *ColDiffReader) expect(s string) error {
	n := len(s)
	b := r.buf.Buffer(n)
	err := r.read(b)
	if err != nil {
		return err
	}
	if string(b) != s {
		return fmt.Errorf("expected %q at position %d, found %q", s, r.off-n, string(b))
	}
	return nil
}

func (r *ColDiffReader) readStrList() ([]string, error) {
	n, sl, err := r.strDec.Read(r.r)
	if err != nil {
		return nil, err
	}
	if len(sl) == 0 {
		sl = nil
	}
	r.off += n
	return sl, nil
}

func (r *ColDiffReader) readUintList() ([]uint32, error) {
	n, sl, err := r.uintDec.Read(r.r)
	if err != nil {
		return nil, err
	}
	if len(sl) == 0 {
		sl = nil
	}
	r.off += n
	return sl, nil
}

func (r *ColDiffReader) readByte() (byte, error) {
	b := r.buf.Buffer(1)
	err := r.read(b)
	return b[0], err
}

func (r *ColDiffReader) readUint32() (uint32, error) {
	b := r.buf.Buffer(4)
	err := r.read(b)
	return binary.BigEndian.Uint32(b), err
}

func (r *ColDiffReader) readNames(c *ColDiff) error {
	err := r.expect("names ")
	if err != nil {
		return err
	}
	names, err := r.readStrList()
	if err != nil {
		return err
	}
	c.Names = names
	return nil
}

func (r *ColDiffReader) readPK(c *ColDiff) error {
	err := r.expect("\npk ")
	if err != nil {
		return err
	}
	pk, err := r.readStrList()
	if err != nil {
		return err
	}
	c.PK = map[string]int{}
	if len(pk) == 0 {
		c.PK = nil
	}
	for i, s := range pk {
		c.PK[s] = i
	}
	return nil
}

func (r *ColDiffReader) readLayers() (int, error) {
	err := r.expect("\nlayers ")
	if err != nil {
		return 0, err
	}
	n, err := r.readByte()
	return int(n), err
}

func (r *ColDiffReader) readIndexSet(label string, layers int) ([]map[uint32]struct{}, error) {
	err := r.expect("\n" + label)
	if err != nil {
		return nil, err
	}
	mapSl := make([]map[uint32]struct{}, layers)
	if len(mapSl) == 0 {
		mapSl = nil
	}
	for i := 0; i < layers; i++ {
		err := r.expect("\n  ")
		if err != nil {
			return nil, err
		}
		sl, err := r.readUintList()
		if err != nil {
			return nil, err
		}
		mapSl[i] = map[uint32]struct{}{}
		for _, u := range sl {
			mapSl[i][u] = struct{}{}
		}
	}
	return mapSl, nil
}

func (r *ColDiffReader) readMovedMap(c *ColDiff, layers int) error {
	err := r.expect("\nmoved")
	if err != nil {
		return err
	}
	c.Moved = make([]map[uint32][]int, layers)
	if len(c.Moved) == 0 {
		c.Moved = nil
	}
	for i := 0; i < layers; i++ {
		err := r.expect("\n  ")
		if err != nil {
			return err
		}
		n, err := r.readUint32()
		if err != nil {
			return err
		}
		c.Moved[i] = map[uint32][]int{}
		for j := 0; j < int(n); j++ {
			u, err := r.readUint32()
			if err != nil {
				return err
			}
			s, err := r.readByte()
			if err != nil {
				return err
			}
			m, err := r.readUint32()
			if err != nil {
				return err
			}
			if s == 'a' {
				c.Moved[i][u] = []int{-1, int(m)}
			} else {
				c.Moved[i][u] = []int{int(m), -1}
			}
		}
	}
	return nil
}

func (r *ColDiffReader) readIndexMap(label string) (map[uint32]uint32, error) {
	err := r.expect(fmt.Sprintf("\n%s ", label))
	if err != nil {
		return nil, err
	}
	n, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	m := map[uint32]uint32{}
	if n == 0 {
		m = nil
	}
	for i := 0; i < int(n); i++ {
		err = r.expect("\n  ")
		if err != nil {
			return nil, err
		}
		k, err := r.readUint32()
		if err != nil {
			return nil, err
		}
		v, err := r.readUint32()
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}

func (r *ColDiffReader) Read() (n int, c *ColDiff, err error) {
	c = &ColDiff{}
	err = r.readNames(c)
	if err != nil {
		return
	}
	err = r.readPK(c)
	if err != nil {
		return
	}
	layers, err := r.readLayers()
	if err != nil {
		return
	}
	c.Added, err = r.readIndexSet("added", layers)
	if err != nil {
		return
	}
	c.Removed, err = r.readIndexSet("removed", layers)
	if err != nil {
		return
	}
	err = r.readMovedMap(c, layers)
	if err != nil {
		return
	}
	c.BaseIdx, err = r.readIndexMap("baseIdx")
	if err != nil {
		return
	}
	for i := 0; i < layers; i++ {
		idxMap, err := r.readIndexMap("otherIdx")
		if err != nil {
			return 0, nil, err
		}
		c.OtherIdx = append(c.OtherIdx, idxMap)
	}
	err = r.expect("\n")
	if err != nil {
		return
	}
	return r.off, c, nil
}
