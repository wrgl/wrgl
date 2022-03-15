// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objline

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/wrgl/wrgl/pkg/encoding"
)

func WriteString(w io.Writer, buf encoding.Bufferer, s string) (n int64, err error) {
	b := buf.Buffer(2)
	l := uint16(len(s))
	binary.BigEndian.PutUint16(b, l)
	m, err := w.Write(b)
	if err != nil {
		return 0, err
	}
	n = int64(m)
	m, err = w.Write([]byte(s))
	if err != nil {
		return 0, err
	}
	n += int64(m)
	return
}

func ReadString(p *encoding.Parser, s *string) (n int64, err error) {
	b, err := p.NextBytes(2)
	if err != nil {
		return 0, err
	}
	l := binary.BigEndian.Uint16(b)
	b, err = p.NextBytes(int(l))
	if err != nil && err != io.EOF {
		return 0, err
	}
	*s = string(b)
	return 2 + int64(l), err
}

func WriteUint16(w io.Writer, buf encoding.Bufferer, u uint16) (n int64, err error) {
	b := buf.Buffer(2)
	binary.BigEndian.PutUint16(b, u)
	_, err = w.Write(b)
	if err != nil {
		return 0, err
	}
	return 2, nil
}

func ReadUint16(p *encoding.Parser, u *uint16) (n int64, err error) {
	b, err := p.NextBytes(2)
	if err != nil {
		return 0, err
	}
	*u = binary.BigEndian.Uint16(b)
	return 2, nil
}

func WriteUint32(w io.Writer, buf encoding.Bufferer, u uint32) (n int64, err error) {
	b := buf.Buffer(4)
	binary.BigEndian.PutUint32(b, u)
	_, err = w.Write(b)
	if err != nil {
		return 0, err
	}
	return 4, nil
}

func ReadUint32(p *encoding.Parser, u *uint32) (n int64, err error) {
	b, err := p.NextBytes(4)
	if err != nil {
		return 0, err
	}
	*u = binary.BigEndian.Uint32(b)
	return 4, nil
}

func WriteBool(w io.Writer, buf encoding.Bufferer, v bool) (n int64, err error) {
	b := buf.Buffer(1)
	b[0] = 0
	if v {
		b[0] = 1
	}
	_, err = w.Write(b)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func ReadBool(p *encoding.Parser, v *bool) (n int64, err error) {
	b, err := p.NextBytes(1)
	if err != nil {
		return 0, err
	}
	if b[0] == 0 {
		*v = false
	} else if b[0] == 1 {
		*v = true
	} else {
		return 0, fmt.Errorf("invalid bool byte %d", b[0])
	}
	return 1, nil
}

func WriteFloat64(w io.Writer, buf encoding.Bufferer, f float64) (n int64, err error) {
	b := buf.Buffer(8)
	bits := math.Float64bits(f)
	binary.BigEndian.PutUint64(b, bits)
	_, err = w.Write(b)
	if err != nil {
		return 0, err
	}
	return 8, nil
}

func ReadFloat64(p *encoding.Parser, f *float64) (n int64, err error) {
	b, err := p.NextBytes(8)
	if err != nil {
		return 0, err
	}
	bits := binary.BigEndian.Uint64(b)
	*f = math.Float64frombits(bits)
	return 8, nil
}

func EncodeTime(t time.Time) []byte {
	return []byte(fmt.Sprintf("%010d %s", t.Unix(), t.Format("-0700")))
}

func WriteTime(w io.Writer, buf encoding.Bufferer, t time.Time) (n int64, err error) {
	m, err := w.Write(EncodeTime(t))
	if err != nil {
		return 0, err
	}
	return int64(m), nil
}

func DecodeTime(s string) (t time.Time, err error) {
	sec, err := strconv.ParseInt(s[0:10], 10, 64)
	if err != nil {
		return
	}
	t = time.Unix(sec, 0)
	tz, err := time.Parse("-0700", s[11:16])
	if err != nil {
		return
	}
	t = t.In(tz.Location())
	return
}

func ReadTime(p *encoding.Parser, t *time.Time) (n int64, err error) {
	b, err := p.NextBytes(16)
	if err != nil {
		return 0, err
	}
	*t, err = DecodeTime(string(b))
	if err != nil {
		return
	}
	return 16, nil
}
