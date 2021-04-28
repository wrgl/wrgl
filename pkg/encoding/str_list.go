package encoding

import (
	"encoding/binary"
	"io"
)

// StrListEncoder encodes string slice. Max bytes size for each string is 65536 bytes
type StrListEncoder struct {
	buf []byte
}

func NewStrListEncoder() *StrListEncoder {
	return &StrListEncoder{
		buf: make([]byte, 0, 256),
	}
}

func (e *StrListEncoder) Encode(sl []string) []byte {
	bufLen := 2
	for _, s := range sl {
		bufLen += len(s) + 2
	}
	if bufLen > cap(e.buf) {
		e.buf = make([]byte, bufLen)
	} else {
		e.buf = e.buf[:bufLen]
	}
	var offset uint16
	for _, s := range sl {
		l := uint16(len(s))
		binary.BigEndian.PutUint16(e.buf[offset:], l)
		offset += 2
		copy(e.buf[offset:], []byte(s))
		offset += l
	}
	binary.BigEndian.PutUint16(e.buf[offset:], 0)
	return e.buf
}

// StrListDecoder decodes string slice.
type StrListDecoder struct {
	strs []string
	buf  []byte
	pos  int
}

func NewStrListDecoder(reuseRecords bool) *StrListDecoder {
	d := &StrListDecoder{
		buf: make([]byte, 2),
	}
	if reuseRecords {
		d.strs = make([]string, 0, 256)
	}
	return d
}

func (d *StrListDecoder) strSlice() []string {
	if d.strs != nil {
		return d.strs[:0]
	}
	return []string{}
}

func (d *StrListDecoder) Decode(b []byte) []string {
	var offset uint16
	sl := d.strSlice()
	total := uint16(len(b))
	for {
		if offset >= total {
			break
		}
		l := binary.BigEndian.Uint16(b[offset:])
		if l == 0 {
			break
		}
		offset += 2
		s := make([]byte, l)
		copy(s, b[offset:])
		offset += l
		sl = append(sl, string(s))
	}
	return sl
}

func (d *StrListDecoder) readUint16(r io.Reader) (uint16, error) {
	b := d.buf
	n, err := r.Read(b)
	if err != nil {
		return 0, err
	}
	d.pos += n
	return binary.BigEndian.Uint16(b), nil
}

func (d *StrListDecoder) Read(r io.Reader) (int, []string, error) {
	d.pos = 0
	sl := d.strSlice()
	for {
		l, err := d.readUint16(r)
		if err != nil {
			return d.pos, nil, err
		}
		if l == 0 {
			break
		}
		s := make([]byte, l)
		n, err := r.Read(s)
		if err != nil {
			return d.pos, nil, err
		}
		d.pos += n
		sl = append(sl, string(s))
	}
	return d.pos, sl, nil
}
