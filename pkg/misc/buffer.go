package misc

import (
	"errors"
	"fmt"
	"io"
)

const maxBufferGrow = 1 << 30

type Buffer struct {
	b    []byte
	off  int64
	grow int
}

func NewBuffer(b []byte) *Buffer {
	total := int64(len(b))
	return &Buffer{b: b, grow: 2, off: total}
}

func (b *Buffer) growSize() int {
	b.grow = b.grow << 1
	if b.grow > maxBufferGrow {
		b.grow = maxBufferGrow
	}
	return b.grow
}

func (b *Buffer) maybeGrow(n int) {
	l := len(b.b)
	if n <= l {
		return
	}
	c := cap(b.b)
	if n > c+maxBufferGrow {
		panic(fmt.Sprintf("asking for too much space in advance: %d", n-c))
	}
	newlen := c
	for n > newlen {
		newlen += b.growSize()
	}
	if newlen > c {
		sl := make([]byte, n, newlen)
		copy(sl, b.b)
		b.b = sl
	} else {
		b.b = b.b[:n]
	}
}

func (b *Buffer) Reset() {
	b.b = b.b[:0]
	b.off = 0
}

func (b *Buffer) Write(p []byte) (n int, err error) {
	n, err = b.WriteAt(p, int64(b.off))
	b.off += int64(len(p))
	return
}

func (b *Buffer) WriteAt(p []byte, off int64) (n int, err error) {
	n = len(p)
	b.maybeGrow(n + int(off))
	copy(b.b[off:], p)
	return
}

func (b *Buffer) Bytes() []byte {
	return b.b
}

func (b *Buffer) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	default:
		return 0, errors.New("Seek: invalid whence")
	case io.SeekStart:
		break
	case io.SeekCurrent:
		offset += int64(b.off)
	case io.SeekEnd:
		offset += int64(len(b.b))
	}
	if offset < 0 {
		return 0, errors.New("Seek: invalid offset")
	}
	b.maybeGrow(int(offset) + 1)
	b.off = offset
	return offset, nil
}

func (b *Buffer) Buffer(n int) []byte {
	b.off = 0
	b.maybeGrow(n)
	return b.b[:n]
}
