// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"io"
	"time"

	"github.com/wrgl/wrgl/pkg/encoding"
	"github.com/wrgl/wrgl/pkg/misc"
)

type Commit struct {
	Table       []byte
	AuthorName  string
	AuthorEmail string
	Time        time.Time
	Message     string
	Parents     [][]byte
}

func (c *Commit) WriteTo(w io.Writer) (int64, error) {
	buf := misc.NewBuffer(nil)
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
	var total int64
	for _, l := range lines {
		n, err := writeLine(w, l.label, l.f(buf))
		if err != nil {
			return 0, err
		}
		total += int64(n)
	}
	return total, nil
}

func (c *Commit) ReadFrom(r io.Reader) (int64, error) {
	parser := encoding.NewParser(r)
	c.Table = make([]byte, 16)
	type line struct {
		label string
		f     encoding.DecodeFunc
	}
	var total int64
	for _, l := range []line{
		{"table", encoding.DecodeBytes(c.Table)},
		{"authorName", encoding.DecodeStr(&c.AuthorName)},
		{"authorEmail", encoding.DecodeStr(&c.AuthorEmail)},
		{"time", encoding.DecodeTimeFunc(&c.Time)},
		{"message", encoding.DecodeStr(&c.Message)},
	} {
		n, err := readLine(parser, l.label, l.f)
		if err != nil {
			return 0, err
		}
		total += int64(n)
	}
	for {
		b := make([]byte, 16)
		n, err := readLine(parser, "parent", encoding.DecodeBytes(b))
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		total += int64(n)
		c.Parents = append(c.Parents, b)
	}
	return total, nil
}

func ReadCommitFrom(r io.Reader) (int64, *Commit, error) {
	c := &Commit{}
	n, err := c.ReadFrom(r)
	return n, c, err
}
