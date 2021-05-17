package encoding

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"time"
)

type Bufferer interface {
	Buffer(n int) []byte
}

type EncodeFunc func(w Bufferer) []byte

func EncodeBytes(b []byte) EncodeFunc {
	return func(w Bufferer) []byte {
		return b
	}
}

func EncodeStr(s string) EncodeFunc {
	return func(w Bufferer) []byte {
		b := w.Buffer(len(s) + 2)
		l := uint16(len(s))
		binary.BigEndian.PutUint16(b, l)
		copy(b[2:], []byte(s))
		return b
	}
}

func EncodeTimeFunc(t time.Time) EncodeFunc {
	return func(w Bufferer) []byte {
		return []byte(EncodeTime(t))
	}
}

func EncodeTime(t time.Time) []byte {
	return []byte(fmt.Sprintf("%010d %s", t.Unix(), t.Format("-0700")))
}

type DecodeFunc func(p *Parser) error

func DecodeBytes(b []byte) DecodeFunc {
	return func(p *Parser) error {
		return p.ReadBytes(b)
	}
}

func DecodeStr(s *string) DecodeFunc {
	return func(p *Parser) error {
		b, err := p.NextBytes(2)
		if err != nil {
			return err
		}
		l := binary.BigEndian.Uint16(b)
		b, err = p.NextBytes(int(l))
		if err != nil {
			return err
		}
		*s = string(b)
		return nil
	}
}

func DecodeTimeFunc(t *time.Time) DecodeFunc {
	return func(p *Parser) error {
		b, err := p.NextBytes(16)
		if err != nil {
			return err
		}
		*t, err = DecodeTime(string(b))
		if err != nil {
			return err
		}
		return nil
	}
}

func DecodeTime(s string) (t time.Time, err error) {
	var sec int64
	sec, err = strconv.ParseInt(s[0:10], 10, 64)
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
