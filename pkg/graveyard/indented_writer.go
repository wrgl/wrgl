// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package graveyard

import (
	"bytes"
	"io"
	"strings"
)

type IndentedWriter struct {
	indent int
	w      io.Writer
}

func (w *IndentedWriter) Write(b []byte) (int, error) {
	b = bytes.ReplaceAll(b, []byte("\n"), []byte("\n"+strings.Repeat(" ", w.indent)))
	return w.w.Write(b)
}

func NewIndentedWriter(w io.Writer, indent int) *IndentedWriter {
	return &IndentedWriter{
		w:      w,
		indent: indent,
	}
}
