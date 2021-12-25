// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package graveyard

import (
	"fmt"
	"io"
	"strings"
)

// TODO: write tests

type SimpleTableWriter struct {
	w              io.Writer
	nrows, ncols   int
	indent, spaces int
	colAlignRight  []bool
	width          func(y, x int) int
	value          func(y, x int) string
}

type SimpleTableWriterOption func(p *SimpleTableWriter)

func WithWidthGetter(f func(y, x int) int) SimpleTableWriterOption {
	return func(p *SimpleTableWriter) {
		p.width = f
	}
}

func WithRightAlignedColumns(rightAligned []bool) SimpleTableWriterOption {
	return func(p *SimpleTableWriter) {
		p.colAlignRight = rightAligned
	}
}

func WithInbetweenSpaces(spaces int) SimpleTableWriterOption {
	return func(p *SimpleTableWriter) {
		p.spaces = spaces
	}
}

func WithIndent(indent int) SimpleTableWriterOption {
	return func(p *SimpleTableWriter) {
		p.indent = indent
	}
}

func NewSimpleTableWriter(w io.Writer, nrows, ncols int, value func(y, x int) string, opts ...SimpleTableWriterOption) *SimpleTableWriter {
	p := &SimpleTableWriter{
		w:      w,
		nrows:  nrows,
		ncols:  ncols,
		value:  value,
		spaces: 1,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// PrintSimpleTable prints simple table from rows of text
func (p *SimpleTableWriter) Write() error {
	maxWidths := []int{}
	widths := make([][]int, p.nrows)
	for j := 0; j < p.nrows; j++ {
		widths[j] = make([]int, p.ncols)
		for i := 0; i < p.ncols; i++ {
			var n int
			if p.width != nil {
				n = p.width(j, i)
			} else {
				n = len(p.value(j, i))
			}
			widths[j][i] = n
			if i >= len(maxWidths) {
				maxWidths = append(maxWidths, n)
			} else if maxWidths[i] < n {
				maxWidths[i] = n
			}
		}
	}
	sum := 0
	for _, w := range maxWidths {
		sum += w
	}
	for j := 0; j < p.nrows; j++ {
		if p.indent > 0 {
			if _, err := fmt.Fprint(p.w, strings.Repeat(" ", p.indent)); err != nil {
				return err
			}
		}
		for i := 0; i < p.ncols; i++ {
			cell := p.value(j, i)
			if p.colAlignRight != nil && p.colAlignRight[i] {
				if _, err := fmt.Fprint(p.w, strings.Repeat(" ", maxWidths[i]-widths[j][i])); err != nil {
					return err
				}
				if _, err := fmt.Fprint(p.w, cell); err != nil {
					return err
				}
			} else {
				if _, err := fmt.Fprint(p.w, cell); err != nil {
					return err
				}
				if _, err := fmt.Fprint(p.w, strings.Repeat(" ", maxWidths[i]-widths[j][i])); err != nil {
					return err
				}
			}
			if i == p.ncols-1 {
				if _, err := fmt.Fprint(p.w, "\n"); err != nil {
					return err
				}
			} else {
				if _, err := fmt.Fprint(p.w, strings.Repeat(" ", p.spaces)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
