// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package table

import (
	"io"

	"github.com/wrgl/core/pkg/objects"
)

type RowHashReader interface {
	Read() (pkHash, rowHash []byte, err error)
}

type rowHashReader struct {
	reader *objects.TableReader
	offset int
	size   int
	n      int
}

func newRowHashReader(reader *objects.TableReader, n, offset, size int) RowHashReader {
	offset, size = capSize(n, offset, size)
	return &rowHashReader{
		reader: reader,
		offset: offset,
		size:   size,
		n:      -1,
	}
}

func (r *rowHashReader) Read() (pkHash, rowHash []byte, err error) {
	r.n++
	if r.n >= r.size {
		return nil, nil, io.EOF
	}
	kh, err := r.reader.ReadRowAt(r.offset + r.n)
	if err != nil {
		return nil, nil, err
	}
	return kh[:16], kh[16:], nil
}
