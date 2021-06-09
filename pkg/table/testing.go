// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package table

import (
	"fmt"
	"io"

	"github.com/wrgl/core/pkg/slice"
)

type MockStore struct {
	columns    []string
	primaryKey []uint32
	rows       [][2]string
}

func NewMockStore(columns []string, primaryKey []uint32, rows [][2]string) *MockStore {
	return &MockStore{
		columns:    columns,
		primaryKey: primaryKey,
		rows:       rows,
	}
}

func (s *MockStore) Close() error {
	return nil
}

func (s *MockStore) InsertRow(n int, pkHash, rowHash, rowContent []byte) error {
	return nil
}

func (s *MockStore) Columns() []string {
	return s.columns
}

func (s *MockStore) PrimaryKey() []string {
	return slice.IndicesToValues(s.columns, s.primaryKey)
}

func (s *MockStore) PrimaryKeyIndices() []uint32 {
	return s.primaryKey
}

func (s *MockStore) GetRowHash(pkHash []byte) (rowHash []byte, ok bool) {
	k := string(pkHash)
	for _, row := range s.rows {
		if row[0] == k {
			return []byte(row[1]), true
		}
	}
	return nil, false
}

func (s *MockStore) NumRows() int {
	return len(s.rows)
}

type MockRowHashReader struct {
	rows [][2]string
	n    int
}

func (r *MockRowHashReader) Read() (pkHash, rowHash []byte, err error) {
	if r.n >= len(r.rows) {
		return nil, nil, io.EOF
	}
	r.n++
	row := r.rows[r.n-1]
	return []byte(row[0]), []byte(row[1]), nil
}

func (s *MockStore) NewRowHashReader(offset, size int) RowHashReader {
	if size == 0 {
		size = len(s.rows) - offset
	}
	return &MockRowHashReader{rows: s.rows[offset : offset+size]}
}

func (s *MockStore) NewRowReader() RowReader {
	return nil
}

func (s *MockStore) Save() ([]byte, error) {
	return nil, fmt.Errorf("Unimplemented")
}
