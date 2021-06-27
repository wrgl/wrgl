package objects

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

type TableIndex struct {
	filters []*bloom.BloomFilter
	mutex   sync.Mutex
}

func NewTableIndex() *TableIndex {
	return &TableIndex{}
}

func (idx *TableIndex) AddSum(blockOffset int, sum []byte) {
	if blockOffset >= len(idx.filters) {
		idx.mutex.Lock()
		for i := len(idx.filters); i <= blockOffset; i++ {
			// m = 1280 and k = 5 yield p = 0.0094 for maximum of 128 items
			idx.filters = append(idx.filters, bloom.New(1280, 5))
		}
		idx.mutex.Unlock()
	}
	idx.filters[blockOffset].Add(sum)
}

func (idx *TableIndex) BlockOffsetOf(sum []byte) int {
	for i, filter := range idx.filters {
		if filter.Test(sum) {
			return i
		}
	}
	return -1
}

func (idx *TableIndex) WriteTo(w io.Writer) (int64, error) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(len(idx.filters)))
	n, err := w.Write(b)
	if err != nil {
		return 0, err
	}
	total := int64(n)
	for _, filter := range idx.filters {
		n, err := filter.WriteTo(w)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func ReadTableIndex(r io.Reader) (int64, *TableIndex, error) {
	b := make([]byte, 4)
	n, err := r.Read(b)
	if err != nil {
		return 0, nil, err
	}
	total := int64(n)
	l := int(binary.BigEndian.Uint32(b))
	idx := &TableIndex{
		filters: make([]*bloom.BloomFilter, l),
	}
	for i := 0; i < l; i++ {
		idx.filters[i] = &bloom.BloomFilter{}
		n, err := idx.filters[i].ReadFrom(r)
		if err != nil {
			return 0, nil, err
		}
		total += n
	}
	return total, idx, nil
}
