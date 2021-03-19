package table

import (
	"fmt"
	"io"

	"github.com/wrgl/core/pkg/slice"
)

type MockStore struct {
	columns    []string
	primaryKey []int
	rows       [][2]string
}

func NewMockStore(columns []string, primaryKey []int, rows [][2]string) *MockStore {
	return &MockStore{
		columns:    columns,
		primaryKey: primaryKey,
		rows:       rows,
	}
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

func (s *MockStore) GetRowHash(pkHash []byte) (rowHash []byte, ok bool) {
	k := string(pkHash)
	for _, row := range s.rows {
		if row[0] == k {
			return []byte(row[1]), true
		}
	}
	return nil, false
}

func (s *MockStore) NumRows() (int, error) {
	return len(s.rows), nil
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

func (r *MockRowHashReader) Close() error {
	return nil
}

func (s *MockStore) NewRowHashReader(offset, size int) (RowHashReader, error) {
	if size == 0 {
		size = len(s.rows) - offset
	}
	return &MockRowHashReader{rows: s.rows[offset : offset+size]}, nil
}

func (s *MockStore) NewRowReader(offset, size int) (RowReader, error) {
	return nil, fmt.Errorf("Unimplemented")
}

func (s *MockStore) Save() (string, error) {
	return "", fmt.Errorf("Unimplemented")
}
