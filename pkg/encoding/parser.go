// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package encoding

import (
	"fmt"
	"io"

	"github.com/wrgl/wrgl/pkg/misc"
)

// Parser keep track of read position and read into a buffer
type Parser struct {
	pos int
	buf Bufferer
	r   io.Reader
}

func NewParser(r io.Reader) *Parser {
	return &Parser{
		r:   r,
		buf: misc.NewBuffer(nil),
	}
}

func (r *Parser) ParseError(format string, a ...interface{}) error {
	return fmt.Errorf("parse error at pos=%d: %s", r.pos, fmt.Sprintf(format, a...))
}

func (r *Parser) NextBytes(n int) ([]byte, error) {
	b := r.buf.Buffer(n)
	_, err := r.Read(b)
	return b, err
}

func (r *Parser) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)
	r.pos += n
	return n, err
}
