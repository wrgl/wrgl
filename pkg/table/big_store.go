// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package table

import (
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/slice"
)

type BigStore struct {
	reader *objects.TableReader
	index  *HashIndex
	db     kv.DB
}

func NewBigStore(db kv.DB, reader *objects.TableReader, index *HashIndex) *BigStore {
	return &BigStore{
		db:     db,
		reader: reader,
		index:  index,
	}
}

func (s *BigStore) Columns() []string {
	return s.reader.Columns
}

func (s *BigStore) PrimaryKey() []string {
	return slice.IndicesToValues(s.reader.Columns, s.reader.PK)
}

func (s *BigStore) PrimaryKeyIndices() []uint32 {
	return s.reader.PK
}

func (s *BigStore) GetRowHash(pkHash []byte) (rowHash []byte, ok bool) {
	off, err := s.index.IndexOf(pkHash)
	if err != nil {
		panic(err)
	}
	if off == -1 {
		return nil, false
	}
	b, err := s.reader.ReadRowAt(off)
	if err != nil {
		panic(err)
	}
	return b[16:], true
}

func (s *BigStore) NumRows() int {
	return s.reader.RowsCount()
}

func (s *BigStore) NewRowHashReader(offset, size int) RowHashReader {
	return newRowHashReader(s.reader, s.NumRows(), offset, size)
}

func (s *BigStore) NewRowReader() RowReader {
	return &rowReader{
		reader: s.reader,
		db:     s.db,
		limit:  s.NumRows(),
	}
}
