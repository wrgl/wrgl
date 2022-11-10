package pbar

import (
	"io"
)

type reader struct {
	bar Bar
	r   io.Reader
}

func (r *reader) Read(b []byte) (n int, err error) {
	n, err = r.r.Read(b)
	if n > 0 {
		r.bar.IncrBy(n)
	}
	if err != nil {
		if err == io.EOF {
			r.bar.Done()
		} else {
			r.bar.Abort()
		}
	}
	return
}

func NewReader(bar Bar, r io.Reader) io.Reader {
	return &reader{bar: bar, r: r}
}
