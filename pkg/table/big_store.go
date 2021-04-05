package table

import (
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/slice"
	"google.golang.org/protobuf/proto"
)

const bufferSize = 256 * 1024

var bigTablePrefix = []byte("big_tables/")

func bigTableKey(hash string) []byte {
	return append(bigTablePrefix, hash...)
}

type BigStore struct {
	db    kv.Store
	seed  uint64
	table *objects.BigTable
	rs    *bigRowStore
}

func NewBigStore(db kv.Store, fs kv.FileStore, columns []string, primaryKeyIndices []uint32, seed uint64) (Store, error) {
	rlID, err := generateRowStoreID(fs)
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
		table: &objects.BigTable{
			Columns:    columns,
			Pk:         primaryKeyIndices,
			RowStoreId: rlID,
		},
		rs: rs,
	}, nil
}

func (s *BigStore) Columns() []string {
	return s.table.Columns
}

func (s *BigStore) PrimaryKey() []string {
	return slice.IndicesToValues(s.table.Columns, s.table.Pk)
}

func (s *BigStore) PrimaryKeyIndices() []uint32 {
	return s.table.Pk
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

func (s *BigStore) NewRowReader() (RowReader, error) {
	return s.rs.NewRowReader()
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
	sum, err := hashTable(s.seed, s.table.Columns, s.table.Pk, r)
	if err != nil {
		return "", err
	}
	v, err := proto.Marshal(s.table)
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
	t := new(objects.BigTable)
	err = proto.Unmarshal(v, t)
	if err != nil {
		return nil, err
	}
	rs := &bigRowStore{
		size: bufferSize,
		db:   db,
		fs:   fs,
		id:   t.RowStoreId,
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
	t := new(objects.BigTable)
	err = proto.Unmarshal(v, t)
	if err != nil {
		return err
	}
	rs := &bigRowStore{
		db: db,
		fs: fs,
		id: t.RowStoreId,
	}
	err = rs.Delete()
	if err != nil {
		return err
	}
	return db.Delete(bigTableKey(hash))
}

func GetAllBigTableHashes(db kv.DB) ([]string, error) {
	sl, err := db.FilterKey(bigTablePrefix)
	if err != nil {
		return nil, err
	}
	l := len(bigTablePrefix)
	result := []string{}
	for _, h := range sl {
		result = slice.InsertToSortedStringSlice(result, h[l:])
	}
	return result, nil
}
