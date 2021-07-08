// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"encoding/binary"
	"io"
)

const BlockSize = 255

func WriteBlockTo(enc *StrListEncoder, w io.Writer, blk [][]string) (int64, error) {
	n := len(blk)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(n))
	_, err := w.Write(b)
	if err != nil {
		return 0, err
	}
	var total int64 = 4
	for _, line := range blk {
		b := enc.Encode(line)
		m, err := w.Write(b)
		if err != nil {
			return 0, err
		}
		total += int64(m)
	}
	return total, nil
}

func ReadBlockFrom(r io.Reader) (int64, [][]string, error) {
	b := make([]byte, 4)
	m, err := r.Read(b)
	if err != nil {
		return 0, nil, err
	}
	total := int64(m)
	n := binary.BigEndian.Uint32(b)
	blk := make([][]string, n)
	var i uint32
	dec := NewStrListDecoder(false)
	for i = 0; i < n; i++ {
		m, line, err := dec.Read(r)
		if err != nil {
			return 0, nil, err
		}
		blk[i] = line
		total += int64(m)
	}
	return total, blk, nil
}
