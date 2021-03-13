package table

import (
	"bytes"
	"encoding/gob"
	"io"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/slice"
)

func inMemoryTableKey(hash string) []byte {
	return []byte("tables/" + hash)
}

type KeyHash struct {
	K string
	V []byte
}

type inMemoryTable struct {
	Columns     []string
	PrimaryKeys []int
	Rows        []KeyHash
}

func (t *inMemoryTable) InsertRow(n int, pkHash, rowHash []byte) {
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

func (t *inMemoryTable) RowsMap() map[string][]byte {
	res := make(map[string][]byte, len(t.Rows))
	for _, r := range t.Rows {
		res[r.K] = r.V
	}
	return res
}

func (t *inMemoryTable) NumRows() int {
	return len(t.Rows)
}

func (t *inMemoryTable) PrimaryKeyStrings() []string {
	return slice.IndicesToValues(t.Columns, t.PrimaryKeys)
}

func (t *inMemoryTable) encode() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(t)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeInMemoryTable(data []byte) (*inMemoryTable, error) {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	t := &inMemoryTable{}
	err := decoder.Decode(t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

type inMemoryRowHashReader struct {
	store  *InMemoryStore
	offset int
	size   int
	n      int
}

func (r *inMemoryRowHashReader) Read() (pkHash, rowHash []byte, err error) {
	r.n++
	if r.n >= r.size {
		return nil, nil, io.EOF
	}
	kh := r.store.table.Rows[r.offset+r.n]
	return []byte(kh.K), kh.V, nil
}

type InMemoryStore struct {
	db      kv.DB
	table   *inMemoryTable
	rowsMap map[string][]byte
	seed    uint64
}

func NewInMemoryStore(db kv.DB, columns []string, primaryKeyIndices []int, seed uint64) Store {
	return &InMemoryStore{
		db: db,
		table: &inMemoryTable{
			Columns:     columns,
			PrimaryKeys: primaryKeyIndices,
			Rows:        []KeyHash{},
		},
		seed: seed,
	}
}

func (s *InMemoryStore) Columns() []string {
	return s.table.Columns
}

func (s *InMemoryStore) PrimaryKey() []string {
	return s.table.PrimaryKeyStrings()
}

func (s *InMemoryStore) InsertRow(n int, pkHash, rowHash, rowContent []byte) error {
	s.table.InsertRow(n, pkHash, rowHash)
	return SaveRow(s.db, rowHash, rowContent)
}

func (s *InMemoryStore) GetRowHash(pkHash []byte) (rowHash []byte, ok bool) {
	if s.rowsMap == nil {
		s.rowsMap = s.table.RowsMap()
	}
	rowHash, ok = s.rowsMap[string(pkHash)]
	return
}

func (s *InMemoryStore) NumRows() int {
	return s.table.NumRows()
}

func (s *InMemoryStore) NewRowHashReader(offset, size int) RowHashReader {
	l := s.table.NumRows()
	if size == 0 || l < offset+size {
		size = l - offset
	}
	if size < 0 {
		size = 0
	}
	return &inMemoryRowHashReader{
		store:  s,
		offset: offset,
		size:   size,
		n:      -1,
	}
}

func (s *InMemoryStore) Save() (string, error) {
	sum, err := hashTable(s.seed, s.table.Columns, s.table.PrimaryKeys, s.NewRowHashReader(0, 0))
	if err != nil {
		return "", err
	}
	v, err := s.table.encode()
	if err != nil {
		return "", err
	}
	return sum, s.db.Set(inMemoryTableKey(sum), v)
}

func ReadInMemoryStore(s kv.DB, seed uint64, hash string) (*InMemoryStore, error) {
	v, err := s.Get(inMemoryTableKey(hash))
	if err != nil {
		return nil, err
	}
	var t *inMemoryTable
	t, err = decodeInMemoryTable(v)
	if err != nil {
		return nil, err
	}
	return &InMemoryStore{
		db:    s,
		table: t,
		seed:  seed,
	}, nil
}

func DeleteInMemoryStore(db kv.DB, hash string) error {
	return db.Delete(inMemoryTableKey(hash))
}
