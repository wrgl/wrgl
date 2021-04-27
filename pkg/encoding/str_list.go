package encoding

import "encoding/binary"

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
	bufLen := 0
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
	return e.buf
}

// StrListDecoder decodes string slice.
type StrListDecoder struct {
	strs []string
}

func NewStrListDecoder(reuseRecords bool) *StrListDecoder {
	d := &StrListDecoder{}
	if reuseRecords {
		d.strs = make([]string, 0, 256)
	}
	return d
}

func (d *StrListDecoder) Decode(b []byte) []string {
	var offset uint16
	var sl []string
	if d.strs == nil {
		sl = []string{}
	} else {
		sl = d.strs[:0]
	}
	total := uint16(len(b))
	for {
		if offset >= total {
			break
		}
		l := binary.BigEndian.Uint16(b[offset:])
		offset += 2
		s := make([]byte, l)
		copy(s, b[offset:])
		offset += l
		sl = append(sl, string(s))
	}
	return sl
}
