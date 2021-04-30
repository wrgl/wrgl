package objects

import (
	"fmt"
	"io"
	"time"
)

type Commit struct {
	Table       [16]byte
	AuthorName  string
	AuthorEmail string
	Time        time.Time
	Message     string
	Parents     [][16]byte
}

type CommitWriter struct {
	w   io.Writer
	buf []byte
}

func NewCommitWriter(w io.Writer) *CommitWriter {
	return &CommitWriter{
		w:   w,
		buf: make([]byte, 128),
	}
}

func (r *CommitWriter) Buffer(n int) []byte {
	if n > cap(r.buf) {
		r.buf = make([]byte, n)
	}
	return r.buf[:n]
}

func (w *CommitWriter) Write(c *Commit) (err error) {
	type line struct {
		label string
		f     encodeFunc
	}
	lines := []line{
		{"table", encodeBytes(c.Table[:])},
		{"authorName", encodeStr(c.AuthorName)},
		{"authorEmail", encodeStr(c.AuthorEmail)},
		{"time", encodeTime(c.Time)},
		{"message", encodeStr(c.Message)},
	}
	for _, parent := range c.Parents {
		lines = append(lines, line{"parent", encodeBytes(parent[:])})
	}
	for _, l := range lines {
		err = writeLine(w.w, l.label, l.f(w))
		if err != nil {
			return
		}
	}
	return
}

type CommitReader struct {
	r   io.Reader
	buf []byte
	pos int
}

func NewCommitReader(r io.Reader) *CommitReader {
	return &CommitReader{
		r:   r,
		buf: make([]byte, 128),
	}
}

func (r *CommitReader) ParseError(format string, a ...interface{}) error {
	return fmt.Errorf("parse error at pos=%d: %s", r.pos, fmt.Sprintf(format, a...))
}

func (r *CommitReader) NextBytes(n int) ([]byte, error) {
	if n > cap(r.buf) {
		r.buf = make([]byte, n)
	}
	b := r.buf[:n]
	err := r.ReadBytes(b)
	return b, err
}

func (r *CommitReader) ReadBytes(b []byte) error {
	n, err := r.r.Read(b)
	r.pos += n
	return err
}

func (r *CommitReader) Read() (*Commit, error) {
	c := &Commit{}
	type line struct {
		label string
		f     decodeFunc
	}
	for _, l := range []line{
		{"table", decodeBytes(c.Table[:])},
		{"authorName", decodeStr(&c.AuthorName)},
		{"authorEmail", decodeStr(&c.AuthorEmail)},
		{"time", decodeTime(&c.Time)},
		{"message", decodeStr(&c.Message)},
	} {
		err := readLine(r, l.label, l.f)
		if err != nil {
			return nil, err
		}
	}
	for {
		b := [16]byte{}
		err := readLine(r, "parent", decodeBytes(b[:]))
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		c.Parents = append(c.Parents, b)
	}
	return c, nil
}
