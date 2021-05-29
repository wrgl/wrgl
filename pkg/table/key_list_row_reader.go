// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package table

import (
	"fmt"
	"io"

	"github.com/wrgl/core/pkg/kv"
)

type KeyListRowReader struct {
	db   kv.DB
	keys [][]byte
	off  int
}

func NewKeyListRowReader(db kv.DB, keys [][]byte) *KeyListRowReader {
	return &KeyListRowReader{
		keys: keys,
		db:   db,
	}
}

func (r *KeyListRowReader) Add(key []byte) {
	r.keys = append(r.keys, key)
}

func (r *KeyListRowReader) Read() (rowHash, rowContent []byte, err error) {
	if r.off >= len(r.keys) {
		return nil, nil, io.EOF
	}
	b := r.keys[r.off]
	rc, err := GetRow(r.db, b)
	if err != nil {
		return nil, nil, err
	}
	r.off++
	return b, rc, nil
}

func (r *KeyListRowReader) Seek(offset int, whence int) (int, error) {
	switch whence {
	default:
		return 0, fmt.Errorf("Seek: invalid whence")
	case io.SeekStart:
		break
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += len(r.keys)
	}
	if offset < 0 {
		return 0, fmt.Errorf("Seek: invalid offset")
	}
	r.off = offset
	return offset, nil
}

func (r *KeyListRowReader) ReadAt(offset int) (rowHash, rowContent []byte, err error) {
	b := r.keys[offset]
	rc, err := GetRow(r.db, b)
	if err != nil {
		return nil, nil, err
	}
	return b, rc, nil
}

func (r *KeyListRowReader) NumRows() int {
	return len(r.keys)
}
