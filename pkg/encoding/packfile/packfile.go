// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package packfile

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/wrgl/wrgl/pkg/encoding"
	"github.com/wrgl/wrgl/pkg/misc"
)

const Version uint32 = 1

const (
	ObjectCommit int = iota + 1
	ObjectTable
	ObjectBlock
)

var typeStrs = map[int]string{
	ObjectCommit: "commit",
	ObjectTable:  "table",
	ObjectBlock:  "block",
}

func encodeObjTypeAndLen(buf encoding.Bufferer, objType int, u uint64) []byte {
	bits := int(math.Floor(math.Log2(float64(u)) + 1))
	numBytes := (bits-4)/7 + 1
	if (bits-4)%7 > 0 {
		numBytes += 1
	}
	if numBytes == 1 {
		numBytes = 2
	}
	b := buf.Buffer(numBytes)
	b[0] = 128 | uint8(objType)<<4 | (uint8(u) & 15)
	bits = 4
	for i := 1; i < numBytes; i++ {
		b[i] = 128 | uint8(u>>bits)
		bits += 7
	}
	b[numBytes-1] &= 127
	return b
}

func decodeObjTypeAndLen(r io.Reader) (objType int, u uint64, err error) {
	b := make([]byte, 1)
	_, err = r.Read(b)
	if err != nil {
		return
	}
	objType = int((b[0] >> 4) & 7)
	u = uint64(b[0] & 15)
	bits := 4
	for {
		_, err = r.Read(b)
		if errors.Is(err, io.EOF) {
			return 0, 0, fmt.Errorf("reading size: data corrupted")
		}
		if err != nil {
			return
		}
		u |= uint64(b[0]&127) << bits
		if b[0]&128 == 0 {
			break
		}
		bits += 7
	}
	return
}

type PackfileInfo struct {
	Version uint32
	Objects [][2]string
}

func (i *PackfileInfo) AddObject(objType int, sum []byte) error {
	typeStr, ok := typeStrs[objType]
	if !ok {
		return fmt.Errorf("unrecognized object type %d", objType)
	}
	i.Objects = append(i.Objects, [2]string{typeStr, hex.EncodeToString(sum)})
	return nil
}

type PackfileWriter struct {
	w    io.Writer
	buf  encoding.Bufferer
	Info *PackfileInfo
}

func NewPackfileWriter(w io.Writer) (*PackfileWriter, error) {
	pw := &PackfileWriter{
		w:   w,
		buf: misc.NewBuffer(nil),
		Info: &PackfileInfo{
			Version: Version,
		},
	}
	err := pw.writeVersion()
	if err != nil {
		return nil, err
	}
	return pw, nil
}

func (w *PackfileWriter) writeVersion() error {
	b := w.buf.Buffer(8)
	copy(b[:4], []byte("PACK"))
	binary.BigEndian.PutUint32(b[4:], Version)
	_, err := w.w.Write(b)
	return err
}

func (w *PackfileWriter) WriteObject(objType int, b []byte) (int, error) {
	buf := encodeObjTypeAndLen(w.buf, objType, uint64(len(b)))
	total, err := w.w.Write(buf)
	if err != nil {
		return 0, err
	}
	m, err := w.w.Write(b)
	if err != nil {
		return 0, err
	}
	total += m
	return total, nil
}

type PackfileReader struct {
	r       io.ReadCloser
	buf     encoding.Bufferer
	Version int
	Info    *PackfileInfo
}

func NewPackfileReader(r io.ReadCloser) (*PackfileReader, error) {
	pr := &PackfileReader{
		r:   r,
		buf: misc.NewBuffer(nil),
		Info: &PackfileInfo{
			Version: Version,
		},
	}
	err := pr.readVersion()
	if err != nil {
		return nil, err
	}
	return pr, nil
}

func (r *PackfileReader) readVersion() error {
	b := r.buf.Buffer(4)
	_, err := r.r.Read(b)
	if err != nil {
		return fmt.Errorf("error reading PACK string: %v", err)
	}
	if string(b) != "PACK" {
		return fmt.Errorf("not a packfile")
	}
	_, err = r.r.Read(b)
	if err != nil {
		return fmt.Errorf("error reading packfile version: %v", err)
	}
	r.Version = int(binary.BigEndian.Uint32(b))
	return nil
}

func (r *PackfileReader) ReadObject() (objType int, b []byte, err error) {
	objType, u, err := decodeObjTypeAndLen(r.r)
	if err != nil {
		return
	}
	var read uint64 = 0
	b = make([]byte, int(u))
	for read < u {
		n, err := r.r.Read(b[read:])
		if err != nil && err != io.EOF {
			return 0, nil, err
		}
		read += uint64(n)
		if errors.Is(err, io.EOF) && read < u {
			return 0, nil, io.ErrUnexpectedEOF
		}
	}
	return
}

func (r *PackfileReader) Close() error {
	return r.r.Close()
}
