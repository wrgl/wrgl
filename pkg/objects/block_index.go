// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"

	"github.com/mmcloughlin/meow"
	"github.com/wrgl/core/pkg/slice"
)

type BlockIndex struct {
	sortedOff []uint8
	Rows      [][]byte
}

func PKCheckSum(enc *StrListEncoder, row []string, pk []uint32) []byte {
	sum := meow.Checksum(0, enc.Encode(slice.IndicesToValues(row, pk)))
	return sum[:]
}

func IndexBlock(enc *StrListEncoder, blk [][]string, pk []uint32) *BlockIndex {
	n := len(blk)
	idx := &BlockIndex{
		sortedOff: make([]uint8, n),
		Rows:      make([][]byte, n),
	}
	for i, row := range blk {
		rowSum := meow.Checksum(0, enc.Encode(row))
		pkSum := rowSum[:]
		if len(pk) > 0 {
			pkSum = PKCheckSum(enc, row, pk)
		}
		idx.sortedOff[i] = uint8(i)
		idx.Rows[i] = append(pkSum, rowSum[:]...)
	}
	sort.Sort(idx)
	return idx
}

func IndexBlockFromBytes(dec *StrListDecoder, blk []byte, pk []uint32) (*BlockIndex, error) {
	n := int(binary.BigEndian.Uint32(blk))
	idx := &BlockIndex{
		sortedOff: make([]uint8, n),
		Rows:      make([][]byte, n),
	}
	r := bytes.NewReader(blk[4:])
	for i := 0; i < n; i++ {
		_, b, err := dec.ReadBytes(r)
		rowSum := meow.Checksum(0, b)
		pkSum := rowSum[:]
		if len(pk) > 0 {
			b = StrList(b).Pick(pk)
			arr := meow.Checksum(0, b)
			pkSum = arr[:]
		}
		idx.sortedOff[i] = uint8(i)
		idx.Rows[i] = append(pkSum, rowSum[:]...)
		if err == io.EOF && i == n-1 {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	sort.Sort(idx)
	return idx, nil
}

func (idx *BlockIndex) Len() int {
	return len(idx.Rows)
}

func (idx *BlockIndex) Less(i, j int) bool {
	return string(idx.Rows[idx.sortedOff[uint8(i)]][:16]) < string(idx.Rows[idx.sortedOff[uint8(j)]][:16])
}

func (idx *BlockIndex) Swap(i, j int) {
	idx.sortedOff[uint8(i)], idx.sortedOff[uint8(j)] = idx.sortedOff[uint8(j)], idx.sortedOff[uint8(i)]
}

func (idx *BlockIndex) Get(pkSum []byte) (byte, []byte) {
	n := idx.Len()
	i := sort.Search(idx.Len(), func(i int) bool {
		b := idx.Rows[idx.sortedOff[byte(i)]][:16]
		return string(b) >= string(pkSum)
	})
	if i >= n {
		return 0, nil
	}
	j := idx.sortedOff[byte(i)]
	b := idx.Rows[j]
	if bytes.Equal(b[:16], pkSum) {
		return j, b[16:]
	}
	return 0, nil
}

func (idx *BlockIndex) WriteTo(w io.Writer) (int64, error) {
	n := idx.Len()
	b := []byte{byte(n)}
	n, err := w.Write(b)
	if err != nil {
		return 0, err
	}
	total := int64(n)
	n, err = w.Write(idx.sortedOff)
	if err != nil {
		return total, err
	}
	total += int64(n)
	for i := 0; i < n; i++ {
		n, err := w.Write(idx.Rows[i])
		if err != nil {
			return total, err
		}
		total += int64(n)
	}
	return total, nil
}

func (idx *BlockIndex) ReadFrom(r io.Reader) (int64, error) {
	b := []byte{0}
	n, err := r.Read(b)
	if err != nil {
		return 0, err
	}
	total := int64(n)
	l := int(b[0])
	idx.Rows = make([][]byte, l)
	idx.sortedOff = make([]uint8, l)
	n, err = r.Read(idx.sortedOff)
	if err != nil {
		return 0, err
	}
	total += int64(n)
	for i := 0; i < l; i++ {
		idx.Rows[i] = make([]byte, 32)
		n, err = r.Read(idx.Rows[i])
		if err != nil {
			return 0, err
		}
		total += int64(n)
	}
	return total, nil

}

func ReadBlockIndex(r io.Reader) (int64, *BlockIndex, error) {
	idx := &BlockIndex{}
	n, err := idx.ReadFrom(r)
	return n, idx, err
}
