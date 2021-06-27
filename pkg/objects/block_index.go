package objects

import (
	"bytes"
	"io"
	"sort"

	"github.com/mmcloughlin/meow"
	"github.com/wrgl/core/pkg/slice"
)

type BlockIndex struct {
	sortedOff []uint8
	Rows      [][]byte
}

func IndexBlock(blk [][]string, pk []uint32) *BlockIndex {
	n := len(blk)
	idx := &BlockIndex{
		sortedOff: make([]uint8, n),
		Rows:      make([][]byte, n),
	}
	enc := NewStrListEncoder(true)
	for i, row := range blk {
		rowSum := meow.Checksum(0, enc.Encode(row))
		pkSum := rowSum[:]
		if len(pk) > 0 {
			sum := meow.Checksum(0, enc.Encode(slice.IndicesToValues(row, pk)))
			pkSum = sum[:]
		}
		idx.sortedOff[i] = uint8(i)
		idx.Rows[i] = append(pkSum, rowSum[:]...)
	}
	sort.Sort(idx)
	return idx
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

func (idx *BlockIndex) Get(pkSum []byte) []byte {
	n := idx.Len()
	i := sort.Search(idx.Len(), func(i int) bool {
		b := idx.Rows[idx.sortedOff[byte(i)]][:16]
		return string(b) >= string(pkSum)
	})
	if i >= n {
		return nil
	}
	b := idx.Rows[idx.sortedOff[byte(i)]]
	if bytes.Equal(b[:16], pkSum) {
		return b[16:]
	}
	return nil
}

type BlockIndexWriter struct {
	w io.Writer
}

func NewBlockIndexWriter(w io.Writer) *BlockIndexWriter {
	return &BlockIndexWriter{
		w: w,
	}
}

func (w *BlockIndexWriter) Write(idx *BlockIndex) (int, error) {
	n := idx.Len()
	b := []byte{byte(n)}
	total := 0
	n, err := w.w.Write(b)
	if err != nil {
		return total, err
	}
	total += n
	n, err = w.w.Write(idx.sortedOff)
	if err != nil {
		return total, err
	}
	total += n
	for i := 0; i < n; i++ {
		n, err := w.w.Write(idx.Rows[i])
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

type BlockIndexReader struct {
	r io.Reader
}

func NewBlockIndexReader(r io.Reader) *BlockIndexReader {
	return &BlockIndexReader{
		r: r,
	}
}

func (r *BlockIndexReader) Read() (*BlockIndex, error) {
	b := []byte{0}
	_, err := r.r.Read(b)
	if err != nil {
		return nil, err
	}
	n := int(b[0])
	idx := &BlockIndex{
		Rows:      make([][]byte, n),
		sortedOff: make([]uint8, n),
	}
	_, err = r.r.Read(idx.sortedOff)
	if err != nil {
		return nil, err
	}
	for i := 0; i < n; i++ {
		idx.Rows[i] = make([]byte, 32)
		_, err = r.r.Read(idx.Rows[i])
		if err != nil {
			return nil, err
		}
	}
	return idx, nil
}
