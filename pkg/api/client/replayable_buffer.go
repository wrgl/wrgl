package apiclient

import (
	"errors"
	"fmt"
	"io"
)

const maxBufferGrow = 1 << 30

type ReplayableBuffer struct {
	b    []byte
	off  int64
	len  int64
	grow int
}

func NewReplayableBuffer() *ReplayableBuffer {
	return &ReplayableBuffer{grow: 2}
}

func (b *ReplayableBuffer) growSize() int {
	b.grow = b.grow << 1
	if b.grow > maxBufferGrow {
		b.grow = maxBufferGrow
	}
	return b.grow
}

func (b *ReplayableBuffer) maybeGrow(n int) {
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

func (b *ReplayableBuffer) Reset() {
	b.b = b.b[:0]
	b.off = 0
	b.len = 0
}

func (b *ReplayableBuffer) Write(p []byte) (n int, err error) {
	n = len(p)
	b.maybeGrow(n + int(b.off))
	copy(b.b[b.off:], p)
	b.off += int64(len(p))
	b.len = b.off
	return
}

func (b *ReplayableBuffer) Read(p []byte) (n int, err error) {
	if b == nil {
		return 0, io.EOF
	}
	n = len(p)
	m := len(b.b) - int(b.off)
	if m < n {
		n = m
		err = io.EOF
	}
	if n == 0 {
		return
	}
	copy(p, b.b[b.off:])
	b.off += int64(n)
	return
}

func (b *ReplayableBuffer) Seek(offset int64, whence int) (int64, error) {
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

func (b *ReplayableBuffer) Close() error {
	return nil
}
