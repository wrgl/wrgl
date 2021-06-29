package objects

import (
	"encoding/binary"
	"io"
)

type BlockWriter struct {
	enc *StrListEncoder
	w   io.Writer
}

func NewBlockWriter(w io.Writer) *BlockWriter {
	return &BlockWriter{
		enc: NewStrListEncoder(false),
		w:   w,
	}
}

func (w *BlockWriter) Write(blk [][]string) (int, error) {
	n := len(blk)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(n))
	total, err := w.w.Write(b)
	if err != nil {
		return total, err
	}
	for _, line := range blk {
		b := w.enc.Encode(line)
		n, err := w.w.Write(b)
		if err != nil {
			return total, err
		}
		total += n
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
