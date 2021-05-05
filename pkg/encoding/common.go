package encoding

import (
	"encoding/binary"
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

func EncodeTime(t time.Time) EncodeFunc {
	return func(w Bufferer) []byte {
		z := t.Format("-0700")
		b := w.Buffer(8 + 5)
		binary.BigEndian.PutUint64(b, uint64(t.Unix()))
		copy(b[8:], []byte(z))
		return b
	}
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

func DecodeTime(t *time.Time) DecodeFunc {
	return func(p *Parser) error {
		b, err := p.NextBytes(8 + 5)
		if err != nil {
			return err
		}
		*t = time.Unix(int64(binary.BigEndian.Uint64(b)), 0)
		tz, err := time.Parse("-0700", string(b[8:]))
		if err != nil {
			return err
		}
		*t = (*t).In(tz.Location())
		return nil
	}
}
