package objects

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

func writeLine(w io.Writer, label string, b []byte) (n int, err error) {
	for _, sl := range [][]byte{
		[]byte(label), {' '}, b, {'\n'},
	} {
		n, err = w.Write(sl)
		if err != nil {
			return
		}
	}
	return
}

type writer interface {
	Buffer(n int) []byte
}

type encodeFunc func(w writer) []byte

func encodeBytes(b []byte) encodeFunc {
	return func(w writer) []byte {
		return b
	}
}

func encodeStr(s string) encodeFunc {
	return func(w writer) []byte {
		b := w.Buffer(len(s) + 2)
		l := uint16(len(s))
		binary.BigEndian.PutUint16(b, l)
		copy(b[2:], []byte(s))
		return b
	}
}

func encodeTime(t time.Time) encodeFunc {
	return func(w writer) []byte {
		z := t.Format("-0700")
		b := w.Buffer(8 + 5)
		binary.BigEndian.PutUint64(b, uint64(t.Unix()))
		copy(b[8:], []byte(z))
		return b
	}
}

type parser interface {
	NextBytes(n int) ([]byte, error)
	ReadBytes(b []byte) error
	ParseError(msg string, a ...interface{}) error
}

type decodeFunc func(p parser) error

func consumeStr(p parser, s string) error {
	b, err := p.NextBytes(len(s))
	if err != nil {
		return err
	}
	if string(b) != s {
		return p.ParseError("expected string %q, received %q", s, string(b))
	}
	return nil
}

func readLine(p parser, label string, f decodeFunc) error {
	err := consumeStr(p, fmt.Sprintf("%s ", label))
	if err == io.EOF {
		return io.EOF
	}
	if err != nil {
		return fmt.Errorf("error reading label %q: %v", label, err)
	}
	err = f(p)
	if err != nil {
		return fmt.Errorf("error reading label %q: %v", label, err)
	}
	err = consumeStr(p, "\n")
	if err != nil {
		return fmt.Errorf("error reading label %q: %v", label, err)
	}
	return nil
}

func decodeBytes(b []byte) decodeFunc {
	return func(p parser) error {
		return p.ReadBytes(b)
	}
}

func decodeStr(s *string) decodeFunc {
	return func(p parser) error {
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

func decodeTime(t *time.Time) decodeFunc {
	return func(p parser) error {
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
