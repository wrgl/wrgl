package table

import (
	"encoding/binary"
	"io"
	"sort"
)

type HashIndexWriter struct {
	fanout  [256]uint32
	offsets []uint32
	hashes  [][]byte
	w       io.Writer
	buf     []byte
}

func NewHashIndexWriter(w io.Writer, rows [][]byte) *HashIndexWriter {
	n := len(rows)
	hashes := make([][]byte, n)
	offsets := make([]uint32, n)
	for i, row := range rows {
		hashes[i] = make([]byte, 16)
		copy(hashes[i], row[:16])
		offsets[i] = uint32(i)
	}
	iw := &HashIndexWriter{
		hashes:  hashes,
		offsets: offsets,
		w:       w,
		buf:     make([]byte, 16),
	}
	sort.Sort(iw)
	iw.computeFanout()
	return iw
}

func (w *HashIndexWriter) Len() int {
	return len(w.offsets)
}

func (w *HashIndexWriter) Less(a, b int) bool {
	for i := 0; i < 16; i++ {
		if w.hashes[a][i] == w.hashes[b][i] {
			continue
		}
		return w.hashes[a][i] < w.hashes[b][i]
	}
	return false
}

func (w *HashIndexWriter) Swap(a, b int) {
	w.hashes[a], w.hashes[b] = w.hashes[b], w.hashes[a]
	w.offsets[a], w.offsets[b] = w.offsets[b], w.offsets[a]
}

func (w *HashIndexWriter) computeFanout() {
	if len(w.hashes) == 0 {
		return
	}
	var b uint8 = w.hashes[0][0]
	for i, s := range w.hashes {
		if s[0] > b {
			for k := b; k < s[0]; k++ {
				w.fanout[k] = uint32(i)
			}
			b = s[0]
		}
	}
	n := uint32(len(w.hashes))
	for k := int(b); k < 256; k++ {
		w.fanout[k] = n
	}
}

func (w *HashIndexWriter) writeUint32(u uint32) error {
	b := w.buf[:4]
	binary.BigEndian.PutUint32(b, u)
	_, err := w.w.Write(b)
	return err
}

func (w *HashIndexWriter) Flush() error {
	for _, off := range w.fanout {
		err := w.writeUint32(off)
		if err != nil {
			return err
		}
	}
	for _, b := range w.hashes {
		_, err := w.w.Write(b)
		if err != nil {
			return err
		}
	}
	for _, off := range w.offsets {
		err := w.writeUint32(off)
		if err != nil {
			return err
		}
	}
	return nil
}

type HashIndex struct {
	size uint32
	r    io.ReadSeeker
	buf  []byte
}

func NewHashIndex(r io.ReadSeeker) (i *HashIndex, err error) {
	i = &HashIndex{
		r:   r,
		buf: make([]byte, 16),
	}
	i.size, err = i.readFanout(255)
	if err != nil {
		return nil, err
	}
	return
}

func (i *HashIndex) readFanout(b uint8) (u uint32, err error) {
	_, err = i.r.Seek(int64(b)*4, io.SeekStart)
	if err != nil {
		return
	}
	_, err = i.r.Read(i.buf)
	if err != nil {
		return
	}
	u = binary.BigEndian.Uint32(i.buf)
	return
}

func (i *HashIndex) readHash(ind uint32) (h []byte, err error) {
	_, err = i.r.Seek(int64(ind)*16+1024, io.SeekStart)
	if err != nil {
		return
	}
	_, err = i.r.Read(i.buf[:16])
	if err != nil {
		return
	}
	h = i.buf[:16]
	return
}

func (i *HashIndex) readOffset(ind uint32) (off int, err error) {
	_, err = i.r.Seek(int64(i.size)*16+int64(ind)*4+1024, io.SeekStart)
	if err != nil {
		return
	}
	_, err = i.r.Read(i.buf[:4])
	if err != nil {
		return
	}
	off = int(binary.BigEndian.Uint32(i.buf))
	return
}

func (i *HashIndex) IndexOf(b []byte) (off int, err error) {
	var startInd, endInd uint32
	if b[0] > 0 {
		startInd, err = i.readFanout(b[0] - 1)
		if err != nil {
			return
		}
	}
	endInd, err = i.readFanout(b[0])
	if err != nil {
		return
	}
	if startInd == endInd {
		return -1, nil
	}
	searchSize := int(endInd - startInd)
	pos := sort.Search(searchSize, func(pos int) bool {
		h, err := i.readHash(startInd + uint32(pos))
		if err != nil {
			panic(err)
		}
		for k := 0; k < 16; k++ {
			if h[k] < b[k] {
				return false
			} else if h[k] > b[k] {
				return true
			}
		}
		return true
	})
	if pos == searchSize {
		return -1, nil
	}
	pos = pos + int(startInd)
	h, err := i.readHash(uint32(pos))
	if err != nil {
		return
	}
	for k := 0; k < 16; k++ {
		if h[k] != b[k] {
			return -1, nil
		}
	}
	return i.readOffset(uint32(pos))
}
