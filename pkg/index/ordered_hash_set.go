package index

import (
	"io"
	"sort"
)

type OrderedHashSetWriter struct {
	fanout  [256]uint32
	offsets []uint32
	hashes  [][]byte
	w       io.Writer
}

func NewOrderedHashSetWriter(w io.Writer, rows [][]byte) *OrderedHashSetWriter {
	n := len(rows)
	hashes := make([][]byte, n)
	offsets := make([]uint32, n)
	for i, row := range rows {
		hashes[i] = make([]byte, 16)
		copy(hashes[i], row[:16])
		offsets[i] = uint32(i)
	}
	iw := &OrderedHashSetWriter{
		hashes:  hashes,
		offsets: offsets,
		w:       w,
	}
	sort.Sort(iw)
	computeFanoutTable(&iw.fanout, iw.hashes)
	return iw
}

func (w *OrderedHashSetWriter) Len() int {
	return len(w.offsets)
}

func (w *OrderedHashSetWriter) Less(a, b int) bool {
	for i := 0; i < 16; i++ {
		if w.hashes[a][i] == w.hashes[b][i] {
			continue
		}
		return w.hashes[a][i] < w.hashes[b][i]
	}
	return false
}

func (w *OrderedHashSetWriter) Swap(a, b int) {
	w.hashes[a], w.hashes[b] = w.hashes[b], w.hashes[a]
	w.offsets[a], w.offsets[b] = w.offsets[b], w.offsets[a]
}

func (w *OrderedHashSetWriter) Flush() error {
	err := writeUint32s(w.w, w.fanout[:])
	if err != nil {
		return err
	}
	for _, b := range w.hashes {
		_, err := w.w.Write(b)
		if err != nil {
			return err
		}
	}
	return writeUint32s(w.w, w.offsets)
}

type OrderedHashSet struct {
	size uint32
	r    io.ReadSeekCloser
	buf  []byte
}

func NewOrderedHashSet(r io.ReadSeekCloser) (s *OrderedHashSet, err error) {
	s = &OrderedHashSet{
		r:   r,
		buf: make([]byte, 16),
	}
	s.size, err = s.readFanout(255)
	if err != nil {
		return nil, err
	}
	return
}

func (s *OrderedHashSet) Close() error {
	return s.r.Close()
}

func (s *OrderedHashSet) readFanout(off byte) (uint32, error) {
	return readUint32(s.r, s.buf, 0, int(off))
}

func (s *OrderedHashSet) readOffset(ind uint32) (off int, err error) {
	u, err := readUint32(s.r, s.buf, 1024+int64(s.size)*16, int(ind))
	return int(u), err
}

func (s *OrderedHashSet) IndexOf(b []byte) (off int, err error) {
	pos, err := indexOf(s.r, s.buf, b)
	if err != nil {
		return
	}
	if pos == -1 {
		return -1, nil
	}
	return s.readOffset(uint32(pos))
}
