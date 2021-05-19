// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package table

import (
	"encoding/hex"
	"io"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/slice"
)

var tablePrefix = []byte("tables/")

func tableKey(hash []byte) []byte {
	return append(tablePrefix, []byte(hex.EncodeToString(hash))...)
}

type KeyHash struct {
	K string
	V []byte
}

type SmallStore struct {
	db      kv.DB
	reader  *objects.TableReader
	rowsMap map[string][]byte
}

func (s *SmallStore) Columns() []string {
	return s.reader.Columns
}

func (s *SmallStore) PrimaryKey() []string {
	return slice.IndicesToValues(s.reader.Columns, s.reader.PK)
}

func (s *SmallStore) PrimaryKeyIndices() []uint32 {
	return s.reader.PK
}

func (s *SmallStore) GetRowHash(pkHash []byte) (rowHash []byte, ok bool) {
	if s.rowsMap == nil {
		s.rowsMap = make(map[string][]byte, s.NumRows())
		s.reader.SeekRow(0, io.SeekStart)
		for {
			b, err := s.reader.ReadRow()
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
			s.rowsMap[string(b[:16])] = append(([]byte)(nil), b[16:]...)
		}
	}
	rowHash, ok = s.rowsMap[string(pkHash)]
	return
}

func (s *SmallStore) NumRows() int {
	return s.reader.RowsCount()
}

func capSize(l, offset, size int) (int, int) {
	if offset < 0 {
		offset = 0
	}
	if size == 0 || l < offset+size {
		size = l - offset
	}
	if size < 0 {
		size = 0
	}
	return offset, size
}

func (s *SmallStore) NewRowHashReader(offset, size int) RowHashReader {
	return newRowHashReader(s.reader, s.NumRows(), offset, size)
}

func (s *SmallStore) NewRowReader() RowReader {
	return &rowReader{
		reader: s.reader,
		db:     s.db,
		limit:  s.NumRows(),
	}
}
