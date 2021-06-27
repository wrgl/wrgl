package objects

import (
	"fmt"
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
	if n > 128 {
		return 0, fmt.Errorf("block size is too big, max size is 128")
	}
	b := []byte{byte(n)}
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

type BlockReader struct {
	dec *StrListDecoder
	r   io.Reader
}

func NewBlockReader(r io.Reader) *BlockReader {
	return &BlockReader{
		dec: NewStrListDecoder(false),
		r:   r,
	}
}

func (r *BlockReader) Read() ([][]string, error) {
	b := make([]byte, 1)
	_, err := r.r.Read(b)
	if err != nil {
		return nil, err
	}
	blk := make([][]string, b[0])
	var i uint8
	for i = 0; i < b[0]; i++ {
		_, line, err := r.dec.Read(r.r)
		if err != nil {
			return nil, err
		}
		fmt.Printf("line: %v\n", line)
		blk[i] = line
	}
	return blk, nil
}
