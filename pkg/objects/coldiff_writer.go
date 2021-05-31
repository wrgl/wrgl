// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/wrgl/core/pkg/misc"
)

type ColDiffWriter struct {
	w       io.Writer
	buf     *misc.Buffer
	strEnc  *StrListEncoder
	uintEnc *UintListEncoder
	off     int
}

func NewColDiffWriter(w io.Writer) *ColDiffWriter {
	return &ColDiffWriter{
		w:       w,
		buf:     misc.NewBuffer(nil),
		strEnc:  NewStrListEncoder(),
		uintEnc: NewUintListEncoder(),
	}
}

func (e *ColDiffWriter) write(b []byte) error {
	n, err := e.w.Write(b)
	if err != nil {
		return err
	}
	e.off += n
	return nil
}

func (e *ColDiffWriter) writeNames(c *ColDiff) error {
	err := e.write([]byte("names "))
	if err != nil {
		return err
	}
	return e.write(e.strEnc.Encode(c.Names))
}

func (e *ColDiffWriter) writePK(c *ColDiff) error {
	err := e.write([]byte("\npk "))
	if err != nil {
		return err
	}
	pk := make([]string, len(c.PK))
	for s, i := range c.PK {
		pk[i] = s
	}
	return e.write(e.strEnc.Encode(pk))
}

func (e *ColDiffWriter) writeLayers(c *ColDiff) error {
	err := e.write([]byte("\nlayers "))
	if err != nil {
		return err
	}
	layers := c.Layers()
	if layers > 255 {
		err = fmt.Errorf("too many layers")
		return err
	}
	return e.write([]byte{byte(layers)})
}

func (e *ColDiffWriter) writeIndexSet(c *ColDiff, label string, mapSl []map[uint32]struct{}) error {
	err := e.write([]byte("\n" + label))
	if err != nil {
		return err
	}
	for i := 0; i < c.Layers(); i++ {
		err = e.write([]byte("\n  "))
		if err != nil {
			return err
		}
		err = e.write(e.uintEnc.Encode(uintMapToSlice(mapSl[i])))
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *ColDiffWriter) writeMovedMap(c *ColDiff) error {
	err := e.write([]byte("\nmoved"))
	if err != nil {
		return err
	}
	for i := 0; i < c.Layers(); i++ {
		n := len(c.Moved[i])
		err = e.write([]byte("\n  "))
		if err != nil {
			return err
		}
		b := e.buf.Buffer(4)
		binary.BigEndian.PutUint32(b, uint32(n))
		err = e.write(b)
		if err != nil {
			return err
		}
		for u, sl := range c.Moved[i] {
			binary.BigEndian.PutUint32(b, u)
			err = e.write(b)
			if err != nil {
				return err
			}
			var s byte
			var m uint32
			if sl[0] == -1 {
				s = 'a'
				m = uint32(sl[1])
			} else {
				s = 'b'
				m = uint32(sl[0])
			}
			err = e.write([]byte{byte(s)})
			if err != nil {
				return err
			}
			binary.BigEndian.PutUint32(b, m)
			err = e.write(b)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *ColDiffWriter) writeIndexMap(c *ColDiff, label string, m map[uint32]uint32) error {
	err := e.write([]byte(fmt.Sprintf("\n%s ", label)))
	if err != nil {
		return err
	}
	b := e.buf.Buffer(4)
	binary.BigEndian.PutUint32(b, uint32(len(m)))
	err = e.write(b)
	if err != nil {
		return err
	}
	for k, v := range m {
		err = e.write([]byte("\n  "))
		if err != nil {
			return err
		}
		for _, u := range []uint32{k, v} {
			binary.BigEndian.PutUint32(b, u)
			err = e.write(b)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *ColDiffWriter) Write(c *ColDiff) (int, error) {
	err := e.writeNames(c)
	if err != nil {
		return 0, err
	}
	err = e.writePK(c)
	if err != nil {
		return 0, err
	}
	err = e.writeLayers(c)
	if err != nil {
		return 0, err
	}
	err = e.writeIndexSet(c, "added", c.Added)
	if err != nil {
		return 0, err
	}
	err = e.writeIndexSet(c, "removed", c.Removed)
	if err != nil {
		return 0, err
	}
	err = e.writeMovedMap(c)
	if err != nil {
		return 0, err
	}
	err = e.writeIndexMap(c, "baseIdx", c.BaseIdx)
	if err != nil {
		return 0, err
	}
	for i := 0; i < c.Layers(); i++ {
		err = e.writeIndexMap(c, "otherIdx", c.OtherIdx[i])
		if err != nil {
			return 0, err
		}
	}
	err = e.write([]byte("\n"))
	if err != nil {
		return 0, err
	}
	return e.off, nil
}

func EncodeColDiff(c *ColDiff) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	w := NewColDiffWriter(buf)
	_, err := w.Write(c)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
