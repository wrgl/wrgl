// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package table

import (
	"fmt"
	"io"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
)

type RowReader interface {
	Read() (rowHash, rowContent []byte, err error)
	Seek(offset int, whence int) (int, error)
	ReadAt(offset int) (rowHash, rowContent []byte, err error)
}

type rowReader struct {
	reader *objects.TableReader
	db     kv.DB
	off    int
	limit  int
}

func (r *rowReader) Read() (rowHash, rowContent []byte, err error) {
	if r.off >= r.limit {
		return nil, nil, io.EOF
	}
	kh, err := r.reader.ReadRowAt(r.off)
	if err != nil {
		return nil, nil, err
	}
	rc, err := GetRow(r.db, kh[16:])
	if err != nil {
		return nil, nil, err
	}
	r.off++
	return kh[16:], rc, nil
}

func (r *rowReader) Seek(offset int, whence int) (int, error) {
	switch whence {
	default:
		return 0, fmt.Errorf("Seek: invalid whence")
	case io.SeekStart:
		break
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += r.limit
	}
	if offset < 0 {
		return 0, fmt.Errorf("Seek: invalid offset")
	}
	r.off = offset
	return offset, nil
}

func (r *rowReader) ReadAt(offset int) (rowHash, rowContent []byte, err error) {
	kh, err := r.reader.ReadRowAt(offset)
	if err != nil {
		return nil, nil, err
	}
	rc, err := GetRow(r.db, kh[16:])
	if err != nil {
		return nil, nil, err
	}
	return kh[16:], rc, nil
}
