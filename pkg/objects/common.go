// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"fmt"
	"io"

	"github.com/wrgl/wrgl/pkg/encoding"
)

func writeLine(w io.Writer, label string, b []byte) (n int, err error) {
	for _, sl := range [][]byte{
		[]byte(label), {' '}, b, {'\n'},
	} {
		m, err := w.Write(sl)
		if err != nil {
			return 0, err
		}
		n += m
	}
	return
}

func consumeStr(p *encoding.Parser, s string) (int, error) {
	n := len(s)
	b, err := p.NextBytes(n)
	if err != nil {
		return 0, err
	}
	if string(b) != s {
		return 0, p.ParseError("expected string %q, received %q", s, string(b))
	}
	return n, nil
}

func readLine(p *encoding.Parser, label string, f encoding.DecodeFunc) (int, error) {
	total, err := consumeStr(p, fmt.Sprintf("%s ", label))
	if err == io.EOF {
		return 0, io.EOF
	}
	if err != nil {
		return 0, fmt.Errorf("error reading label %q: %v", label, err)
	}
	n, err := f(p)
	if err != nil {
		return 0, fmt.Errorf("error reading label %q: %v", label, err)
	}
	total += n
	n, err = consumeStr(p, "\n")
	if err != nil {
		return 0, fmt.Errorf("error reading label %q: %v", label, err)
	}
	total += n
	return total, nil
}
