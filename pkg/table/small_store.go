package table

import (
	"bytes"
	"encoding/gob"
	"io"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/slice"
)

func smallTableKey(hash string) []byte {
	return []byte("tables/" + hash)
}

type KeyHash struct {
	K string
	V []byte
}

type smallTable struct {
	Columns     []string
	PrimaryKeys []int
	Rows        []KeyHash
}

func (t *smallTable) InsertRow(n int, pkHash, rowHash []byte) {
	kh := KeyHash{
		K: string(pkHash),
		V: rowHash,
	}
	oldLen := len(t.Rows)
	if n >= oldLen {
		t.Rows = append(t.Rows, make([]KeyHash, n+1-oldLen)...)
	}
	t.Rows[n] = kh
}

func (t *smallTable) RowsMap() map[string][]byte {
	res := make(map[string][]byte, len(t.Rows))
	for _, r := range t.Rows {
		res[r.K] = r.V
	}
	return res
}

func (t *smallTable) NumRows() int {
	return len(t.Rows)
}

func (t *smallTable) PrimaryKeyStrings() []string {
	return slice.IndicesToValues(t.Columns, t.PrimaryKeys)
}

func (t *smallTable) encode() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(t)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeSmallTable(data []byte) (*smallTable, error) {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	t := &smallTable{}
	err := decoder.Decode(t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

type smallRowHashReader struct {
	store  *SmallStore
	offset int
	size   int
	n      int
}

func (r *smallRowHashReader) Read() (pkHash, rowHash []byte, err error) {
	r.n++
	if r.n >= r.size {
		return nil, nil, io.EOF
	}
	kh := r.store.table.Rows[r.offset+r.n]
	return []byte(kh.K), kh.V, nil
}

func (r *smallRowHashReader) Close() error {
	return nil
}

type smallRowReader struct {
	store  *SmallStore
	offset int
	size   int
	n      int
}

func (r *smallRowReader) Read() (rowHash, rowContent []byte, err error) {
	r.n++
	if r.n >= r.size {
		return nil, nil, io.EOF
	}
	kh := r.store.table.Rows[r.offset+r.n]
	rc, err := GetRow(r.store.db, kh.V)
	if err != nil {
		return nil, nil, err
	}
	return kh.V, rc, nil
}

func (r *smallRowReader) Close() error {
	return nil
}

type SmallStore struct {
	db      kv.DB
	table   *smallTable
	rowsMap map[string][]byte
	seed    uint64
}

func NewSmallStore(db kv.DB, columns []string, primaryKeyIndices []int, seed uint64) Store {
	return &SmallStore{
		db: db,
		table: &smallTable{
			Columns:     columns,
			PrimaryKeys: primaryKeyIndices,
			Rows:        []KeyHash{},
		},
		seed: seed,
	}
}

func (s *SmallStore) Columns() []string {
	return s.table.Columns
}

func (s *SmallStore) PrimaryKey() []string {
	return s.table.PrimaryKeyStrings()
}

func (s *SmallStore) InsertRow(n int, pkHash, rowHash, rowContent []byte) error {
	s.table.InsertRow(n, pkHash, rowHash)
	return SaveRow(s.db, rowHash, rowContent)
}

func (s *SmallStore) GetRowHash(pkHash []byte) (rowHash []byte, ok bool) {
	if s.rowsMap == nil {
		s.rowsMap = s.table.RowsMap()
	}
	rowHash, ok = s.rowsMap[string(pkHash)]
	return
}

func (s *SmallStore) NumRows() (int, error) {
	return s.table.NumRows(), nil
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

func (s *SmallStore) NewRowHashReader(offset, size int) (RowHashReader, error) {
	l := s.table.NumRows()
	offset, size = capSize(l, offset, size)
	return &smallRowHashReader{
		store:  s,
		offset: offset,
		size:   size,
		n:      -1,
	}, nil
}

func (s *SmallStore) NewRowReader(offset, size int) (RowReader, error) {
	l := s.table.NumRows()
	offset, size = capSize(l, offset, size)
	return &smallRowReader{
		store:  s,
		offset: offset,
		size:   size,
		n:      -1,
	}, nil
}

func (s *SmallStore) Save() (string, error) {
	r, err := s.NewRowHashReader(0, 0)
	if err != nil {
		return "", err
	}
	defer r.Close()
	sum, err := hashTable(s.seed, s.table.Columns, s.table.PrimaryKeys, r)
	if err != nil {
		return "", err
	}
	v, err := s.table.encode()
	if err != nil {
		return "", err
	}
	return sum, s.db.Set(smallTableKey(sum), v)
}

func ReadSmallStore(s kv.DB, seed uint64, hash string) (*SmallStore, error) {
	v, err := s.Get(smallTableKey(hash))
	if err != nil {
		return nil, err
	}
	var t *smallTable
	t, err = decodeSmallTable(v)
	if err != nil {
		return nil, err
	}
	return &SmallStore{
		db:    s,
		table: t,
		seed:  seed,
	}, nil
}

func DeleteSmallStore(db kv.DB, hash string) error {
	return db.Delete(smallTableKey(hash))
}
