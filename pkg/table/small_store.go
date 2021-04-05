package table

import (
	"io"
	"sync"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/slice"
	"google.golang.org/protobuf/proto"
)

var smallTablePrefix = []byte("tables/")

func smallTableKey(hash []byte) []byte {
	return append(smallTablePrefix, hash...)
}

type KeyHash struct {
	K string
	V []byte
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
	return kh[:16], kh[16:], nil
}

func (r *smallRowHashReader) Close() error {
	return nil
}

type SmallStore struct {
	db      kv.DB
	table   *objects.SmallTable
	rowsMap map[string][]byte
	seed    uint64
	mu      sync.Mutex
}

func NewSmallStore(db kv.DB, columns []string, primaryKeyIndices []uint32, seed uint64) Store {
	return &SmallStore{
		db: db,
		table: &objects.SmallTable{
			Columns: columns,
			Pk:      primaryKeyIndices,
		},
		seed: seed,
	}
}

func (s *SmallStore) Columns() []string {
	return s.table.Columns
}

func (s *SmallStore) PrimaryKey() []string {
	return slice.IndicesToValues(s.table.Columns, s.table.Pk)
}

func (s *SmallStore) PrimaryKeyIndices() []uint32 {
	return s.table.Pk
}

func (s *SmallStore) InsertRow(n int, pkHash, rowHash, rowContent []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	kh := append(pkHash, rowHash...)
	oldLen := len(s.table.Rows)
	if n >= oldLen {
		s.table.Rows = append(s.table.Rows, make([][]byte, n+1-oldLen)...)
	}
	s.table.Rows[n] = kh
	return SaveRow(s.db, rowHash, rowContent)
}

func (s *SmallStore) GetRowHash(pkHash []byte) (rowHash []byte, ok bool) {
	if s.rowsMap == nil {
		s.rowsMap = make(map[string][]byte, len(s.table.Rows))
		for _, r := range s.table.Rows {
			s.rowsMap[string(r[:16])] = r[16:]
		}
	}
	rowHash, ok = s.rowsMap[string(pkHash)]
	return
}

func (s *SmallStore) NumRows() (int, error) {
	return len(s.table.Rows), nil
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
	l, _ := s.NumRows()
	offset, size = capSize(l, offset, size)
	return &smallRowHashReader{
		store:  s,
		offset: offset,
		size:   size,
		n:      -1,
	}, nil
}

func (s *SmallStore) NewRowReader() (RowReader, error) {
	l, _ := s.NumRows()
	return &smallRowReader{
		store: s,
		limit: l,
	}, nil
}

func (s *SmallStore) Save() ([]byte, error) {
	r, err := s.NewRowHashReader(0, 0)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	sum, err := hashTable(s.seed, s.table.Columns, s.table.Pk, r)
	if err != nil {
		return nil, err
	}
	v, err := proto.Marshal(s.table)
	if err != nil {
		return nil, err
	}
	return sum, s.db.Set(smallTableKey(sum), v)
}

func ReadSmallStore(s kv.DB, seed uint64, hash []byte) (*SmallStore, error) {
	v, err := s.Get(smallTableKey(hash))
	if err != nil {
		return nil, err
	}
	t := new(objects.SmallTable)
	err = proto.Unmarshal(v, t)
	if err != nil {
		return nil, err
	}
	return &SmallStore{
		db:    s,
		table: t,
		seed:  seed,
	}, nil
}

func DeleteSmallStore(db kv.DB, hash []byte) error {
	return db.Delete(smallTableKey(hash))
}

func GetAllSmallTableHashes(db kv.DB) ([][]byte, error) {
	sl, err := db.FilterKey(smallTablePrefix)
	if err != nil {
		return nil, err
	}
	l := len(smallTablePrefix)
	result := [][]byte{}
	for _, h := range sl {
		result = slice.InsertToSortedBytesSlice(result, []byte(h[l:]))
	}
	return result, nil
}
