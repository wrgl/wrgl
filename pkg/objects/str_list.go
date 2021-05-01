package objects

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
	bufLen := 4
	for _, s := range sl {
		bufLen += len(s) + 2
	}
	if bufLen > cap(e.buf) {
		e.buf = make([]byte, bufLen)
	} else {
		e.buf = e.buf[:bufLen]
	}
	binary.BigEndian.PutUint32(e.buf, uint32(len(sl)))
	var offset uint16 = 4
	for _, s := range sl {
		l := uint16(len(s))
		binary.BigEndian.PutUint16(e.buf[offset:], l)
		offset += 2
		copy(e.buf[offset:], []byte(s))
		offset += l
	}
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
		buf: make([]byte, 4),
	}
	if reuseRecords {
		d.strs = make([]string, 0, 256)
	}
	return d
}

func (d *StrListDecoder) strSlice(n uint32) []string {
	if d.strs != nil {
		if n > uint32(cap(d.strs)) {
			d.strs = make([]string, 0, n)
		}
		return d.strs[:0]
	}
	return make([]string, 0, n)
}

func (d *StrListDecoder) Decode(b []byte) []string {
	count := binary.BigEndian.Uint32(b)
	sl := d.strSlice(count)
	var offset uint16 = 4
	var i uint32
	for i = 0; i < count; i++ {
		l := binary.BigEndian.Uint16(b[offset:])
		offset += 2
		if l == 0 {
			sl = append(sl, "")
			continue
		}
		s := make([]byte, l)
		copy(s, b[offset:])
		offset += l
		sl = append(sl, string(s))
	}
	return sl
}

func (d *StrListDecoder) readUint16(r io.Reader) (uint16, error) {
	b := d.buf[:2]
	n, err := r.Read(b)
	if err != nil {
		return 0, err
	}
	d.pos += n
	return binary.BigEndian.Uint16(b), nil
}

func (d *StrListDecoder) readUint32(r io.Reader) (uint32, error) {
	b := d.buf[:4]
	n, err := r.Read(b)
	if err != nil {
		return 0, err
	}
	d.pos += n
	return binary.BigEndian.Uint32(b), nil
}

func (d *StrListDecoder) Read(r io.Reader) (int, []string, error) {
	d.pos = 0
	count, err := d.readUint32(r)
	if err != nil {
		return 0, nil, err
	}
	sl := d.strSlice(count)
	var i uint32
	for i = 0; i < count; i++ {
		l, err := d.readUint16(r)
		if err != nil {
			return d.pos, nil, err
		}
		if l == 0 {
			sl = append(sl, "")
			continue
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
