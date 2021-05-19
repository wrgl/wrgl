// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"io"
	"time"

	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/misc"
)

type Commit struct {
	Table       []byte
	AuthorName  string
	AuthorEmail string
	Time        time.Time
	Message     string
	Parents     [][]byte
}

type CommitWriter struct {
	w   io.Writer
	buf *misc.Buffer
}

func NewCommitWriter(w io.Writer) *CommitWriter {
	return &CommitWriter{
		w:   w,
		buf: misc.NewBuffer(nil),
	}
}

func (w *CommitWriter) Write(c *Commit) (err error) {
	type line struct {
		label string
		f     encoding.EncodeFunc
	}
	lines := []line{
		{"table", encoding.EncodeBytes(c.Table)},
		{"authorName", encoding.EncodeStr(c.AuthorName)},
		{"authorEmail", encoding.EncodeStr(c.AuthorEmail)},
		{"time", encoding.EncodeTimeFunc(c.Time)},
		{"message", encoding.EncodeStr(c.Message)},
	}
	for _, parent := range c.Parents {
		lines = append(lines, line{"parent", encoding.EncodeBytes(parent)})
	}
	for _, l := range lines {
		_, err = writeLine(w.w, l.label, l.f(w.buf))
		if err != nil {
			return
		}
	}
	return
}

type CommitReader struct {
	parser *encoding.Parser
}

func NewCommitReader(r io.Reader) *CommitReader {
	return &CommitReader{
		parser: encoding.NewParser(r),
	}
}

func (r *CommitReader) Read() (*Commit, error) {
	c := &Commit{Table: make([]byte, 16)}
	type line struct {
		label string
		f     encoding.DecodeFunc
	}
	for _, l := range []line{
		{"table", encoding.DecodeBytes(c.Table)},
		{"authorName", encoding.DecodeStr(&c.AuthorName)},
		{"authorEmail", encoding.DecodeStr(&c.AuthorEmail)},
		{"time", encoding.DecodeTimeFunc(&c.Time)},
		{"message", encoding.DecodeStr(&c.Message)},
	} {
		err := readLine(r.parser, l.label, l.f)
		if err != nil {
			return nil, err
		}
	}
	for {
		b := make([]byte, 16)
		err := readLine(r.parser, "parent", encoding.DecodeBytes(b))
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
