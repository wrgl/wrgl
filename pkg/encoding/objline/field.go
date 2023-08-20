// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objline

import (
	"errors"
	"fmt"
	"io"

	"github.com/wrgl/wrgl/pkg/encoding"
)

type WriteFunc func(w io.Writer, buf encoding.Bufferer) (n int64, err error)

type ReadFunc func(p *encoding.Parser) (int64, error)

func WriteField(w io.Writer, buf encoding.Bufferer, label string, f WriteFunc) (n int64, err error) {
	m, err := w.Write(append([]byte(label), ' '))
	if err != nil {
		return 0, err
	}
	n += int64(m)
	l, err := f(w, buf)
	if err != nil {
		return 0, err
	}
	n += l
	m, err = w.Write([]byte{'\n'})
	if err != nil {
		return 0, err
	}
	n += int64(m)
	return
}

func WriteBytes(b []byte) WriteFunc {
	return func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
		m, err := w.Write(b)
		if err != nil {
			return 0, err
		}
		return int64(m), nil
	}
}

func ReadBytes(b []byte) ReadFunc {
	return func(p *encoding.Parser) (int64, error) {
		n, err := p.Read(b)
		if err != nil {
			return 0, err
		}
		return int64(n), nil
	}
}

func consumeStr(p *encoding.Parser, s string) (int64, error) {
	n := len(s)
	b, err := p.NextBytes(n)
	if err != nil {
		return 0, err
	}
	if string(b) != s {
		return 0, p.ParseError("expected string %q, received %q", s, string(b))
	}
	return int64(n), nil
}

func ReadField(p *encoding.Parser, label string, f ReadFunc) (int64, error) {
	total, err := consumeStr(p, fmt.Sprintf("%s ", label))
	if errors.Is(err, io.EOF) {
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
