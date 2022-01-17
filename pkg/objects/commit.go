// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"io"
	"time"

	"github.com/wrgl/wrgl/pkg/encoding"
	"github.com/wrgl/wrgl/pkg/encoding/objline"
	"github.com/wrgl/wrgl/pkg/misc"
)

type Commit struct {
	Sum         []byte
	Table       []byte
	AuthorName  string
	AuthorEmail string
	Time        time.Time
	Message     string
	Parents     [][]byte
}

type fieldEncode struct {
	label string
	f     objline.WriteFunc
}

func (c *Commit) WriteTo(w io.Writer) (int64, error) {
	buf := misc.NewBuffer(nil)
	fields := []fieldEncode{
		{"table", objline.WriteBytes(c.Table)},
		{"authorName", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteString(w, buf, c.AuthorName)
		}},
		{"authorEmail", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteString(w, buf, c.AuthorEmail)
		}},
		{"time", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteTime(w, buf, c.Time)
		}},
		{"message", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteString(w, buf, c.Message)
		}},
	}
	for _, parent := range c.Parents {
		fields = append(fields, fieldEncode{"parent", objline.WriteBytes(parent)})
	}
	var total int64
	for _, l := range fields {
		n, err := objline.WriteField(w, buf, l.label, l.f)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

type fieldDecode struct {
	label string
	f     objline.ReadFunc
}

func (c *Commit) ReadFrom(r io.Reader) (int64, error) {
	parser := encoding.NewParser(r)
	c.Table = make([]byte, 16)
	var total int64
	for _, l := range []fieldDecode{
		{"table", objline.ReadBytes(c.Table)},
		{"authorName", func(p *encoding.Parser) (int64, error) {
			return objline.ReadString(p, &c.AuthorName)
		}},
		{"authorEmail", func(p *encoding.Parser) (int64, error) {
			return objline.ReadString(p, &c.AuthorEmail)
		}},
		{"time", func(p *encoding.Parser) (int64, error) {
			return objline.ReadTime(p, &c.Time)
		}},
		{"message", func(p *encoding.Parser) (int64, error) {
			return objline.ReadString(p, &c.Message)
		}},
	} {
		n, err := objline.ReadField(parser, l.label, l.f)
		if err != nil {
			return 0, err
		}
		total += int64(n)
	}
	for {
		b := make([]byte, 16)
		n, err := objline.ReadField(parser, "parent", objline.ReadBytes(b))
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
