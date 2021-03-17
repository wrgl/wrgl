package table

import (
	"bytes"
	"encoding/gob"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/slice"
)

const bufferSize = 256 * 1024

func bigTableKey(hash string) []byte {
	return []byte("big_tables/" + hash)
}

type bigTable struct {
	Columns     []string
	PrimaryKeys []int
	RowStoreID  []byte
}

func (t *bigTable) PrimaryKeyStrings() []string {
	return slice.IndicesToValues(t.Columns, t.PrimaryKeys)
}

func (t *bigTable) encode() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(t)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeBigTable(data []byte) (*bigTable, error) {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	t := &bigTable{}
	err := decoder.Decode(t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

type BigStore struct {
	db    kv.Store
	seed  uint64
	table *bigTable
	rs    *bigRowStore
}

func NewBigStore(db kv.Store, fs kv.FileStore, columns []string, primaryKeyIndices []int, seed uint64) (Store, error) {
	rlID, err := generateRowStoreID(db)
	if err != nil {
		return nil, err
	}
	rs, err := newBigRowStore(db, fs, rlID, bufferSize)
	if err != nil {
		return nil, err
	}
	return &BigStore{
		db:   db,
		seed: seed,
		table: &bigTable{
			Columns:     columns,
			PrimaryKeys: primaryKeyIndices,
			RowStoreID:  rlID,
		},
		rs: rs,
	}, nil
}

func (s *BigStore) Columns() []string {
	return s.table.Columns
}

func (s *BigStore) PrimaryKey() []string {
	return s.table.PrimaryKeyStrings()
}

func (s *BigStore) InsertRow(n int, pkHash, rowHash, rowContent []byte) error {
	err := s.rs.InsertRow(n, pkHash, rowHash)
	if err != nil {
		return err
	}
	return SaveRow(s.db, rowHash, rowContent)
}

func (s *BigStore) GetRowHash(pkHash []byte) (rowHash []byte, ok bool) {
	return s.rs.GetRowHash(pkHash)
}

func (s *BigStore) NumRows() (int, error) {
	return s.rs.NumRows()
}

func (s *BigStore) NewRowHashReader(offset, size int) (RowHashReader, error) {
	return s.rs.NewRowHashReader(offset, size)
}

func (s *BigStore) NewRowReader(offset, size int) (RowReader, error) {
	return s.rs.NewRowReader(offset, size)
}

func (s *BigStore) Save() (string, error) {
	err := s.rs.Flush()
	if err != nil {
		return "", err
	}
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
	return sum, s.db.Set(bigTableKey(sum), v)
}

func ReadBigStore(db kv.Store, fs kv.FileStore, seed uint64, hash string) (*BigStore, error) {
	v, err := db.Get(bigTableKey(hash))
	if err != nil {
		return nil, err
	}
	t, err := decodeBigTable(v)
	if err != nil {
		return nil, err
	}
	rs := &bigRowStore{
		size: bufferSize,
		db:   db,
		fs:   fs,
		id:   t.RowStoreID,
	}
	return &BigStore{
		db:    db,
		table: t,
		seed:  seed,
		rs:    rs,
	}, nil
}

func DeleteBigStore(db kv.Store, fs kv.FileStore, hash string) error {
	v, err := db.Get(bigTableKey(hash))
	if err != nil {
		return err
	}
	t, err := decodeBigTable(v)
	if err != nil {
		return err
	}
	rs := &bigRowStore{
		db: db,
		fs: fs,
		id: t.RowStoreID,
	}
	err = rs.Delete()
	if err != nil {
		return err
	}
	return db.Delete(bigTableKey(hash))
}
