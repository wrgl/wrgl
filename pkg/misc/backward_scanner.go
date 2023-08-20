// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package misc

import (
	"bytes"
	"errors"
	"io"
)

// BackwardScanner can read line beginning from the end
type BackwardScanner struct {
	buf []byte
	r   io.ReadSeeker
	off int64
}

func NewBackwardScanner(r io.ReadSeeker) (*BackwardScanner, error) {
	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	return &BackwardScanner{
		off: size,
		r:   r,
		buf: make([]byte, 0, 1024),
	}, nil
}

func (s *BackwardScanner) readMore() error {
	if s.off == 0 {
		return io.EOF
	}
	size := 1024
	if size > int(s.off) {
		size = int(s.off)
	}
	newLen := size + len(s.buf)
	var buf []byte
	if newLen > cap(s.buf) {
		buf = make([]byte, newLen)
	} else {
		buf = s.buf[:newLen]
	}
	copy(buf[size:], s.buf)
	_, err := s.r.Seek(-int64(size), io.SeekCurrent)
	if err != nil {
		return err
	}
	_, err = s.r.Read(buf[:size])
	if err != nil {
		return err
	}
	n, err := s.r.Seek(-int64(size), io.SeekCurrent)
	if err != nil {
		return err
	}
	s.off = int64(n)
	s.buf = buf
	return nil
}

func (s *BackwardScanner) ReadLine() (string, error) {
	for {
		start := bytes.LastIndexByte(s.buf, '\n')
		if start != -1 {
			res := string(s.buf[start+1:])
			s.buf = s.buf[:start]
			return res, nil
		}
		err := s.readMore()
		if errors.Is(err, io.EOF) {
			if len(s.buf) > 0 {
				res := string(s.buf)
				s.buf = s.buf[:0]
				return res, nil
			}
		}
		if err != nil {
			return "", err
		}
	}
}
