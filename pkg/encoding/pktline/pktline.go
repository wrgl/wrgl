// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package pktline

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/wrgl/wrgl/pkg/encoding"
)

func WritePktLine(w io.Writer, buf encoding.Bufferer, s string) error {
	n := len(s)
	if n == 0 {
		_, err := w.Write([]byte("0000"))
		return err
	}
	b := buf.Buffer(n + 5)
	copy(b, []byte(fmt.Sprintf("%04x", n+1)))
	copy(b[4:], []byte(s))
	b[4+n] = '\n'
	_, err := w.Write(b)
	return err
}

func ReadPktLine(p *encoding.Parser) (s string, err error) {
	b, err := p.NextBytes(4)
	if err != nil {
		return
	}
	b2 := make([]byte, 2)
	_, err = hex.Decode(b2, b)
	if err != nil {
		return
	}
	u := binary.BigEndian.Uint16(b2)
	if u == 0 {
		return "", nil
	}
	b, err = p.NextBytes(int(u))
	if err != nil {
		return
	}
	s = string(b[:u-1])
	return
}
