package table

import (
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/mmcloughlin/meow"
	"github.com/wrgl/core/pkg/kv"
)

func rowListKey(id []byte) []byte {
	return []byte("rowl/" + hex.EncodeToString(id))
}

func rowMapPrefix(id []byte) []byte {
	return append(append([]byte("rowm/"), id...), "/"[0])
}

func rowMapKey(id []byte, pkSum []byte) []byte {
	return append(rowMapPrefix(id), pkSum...)
}

type bigRowHashReader struct {
	r    io.ReadCloser
	n    int
	size int
}

func (r *bigRowHashReader) Read() (pkHash, rowHash []byte, err error) {
	if r.n >= r.size {
		return nil, nil, io.EOF
	}
	sl := make([]byte, 32)
	_, err = r.r.Read(sl)
	if err != nil {
		return nil, nil, err
	}
	r.n++
	return sl[:16], sl[16:], nil
}

func (r *bigRowHashReader) Close() error {
	return r.r.Close()
}

type bigRowReader struct {
	db   kv.Store
	r    io.ReadCloser
	n    int
	size int
}

func (r *bigRowReader) Read() (rowHash, rowContent []byte, err error) {
	if r.n >= r.size {
		return nil, nil, io.EOF
	}
	sl := make([]byte, 32)
	_, err = r.r.Read(sl)
	if err != nil {
		return nil, nil, err
	}
	r.n++
	rowHash = sl[16:]
	rowContent, err = r.db.Get(rowKey(rowHash))
	if err != nil {
		return nil, nil, err
	}
	return
}

func (r *bigRowReader) Close() error {
	return r.r.Close()
}

type bigRowStore struct {
	mu          sync.Mutex
	size        int
	offset      int
	idxSl       []int
	rowSl       [][]byte
	fs          kv.FileStore
	db          kv.Store
	id          []byte
	rowListFile io.WriteCloser
}

func newBigRowStore(db kv.Store, fs kv.FileStore, id []byte, size int) (*bigRowStore, error) {
	rlf, err := fs.Writer(rowListKey(id))
	if err != nil {
		return nil, err
	}
	return &bigRowStore{
		size:        size,
		db:          db,
		fs:          fs,
		rowListFile: rlf,
		id:          id,
	}, nil
}

func (s *bigRowStore) flushRowListFile(i int) error {
	for _, b := range s.rowSl[:i] {
		_, err := s.rowListFile.Write(b)
		if err != nil {
			return err
		}
	}
	s.offset += i
	s.idxSl = s.idxSl[i:]
	s.rowSl = s.rowSl[i:]
	return nil
}

func (s *bigRowStore) InsertRow(n int, pkHash, rowHash []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := sort.SearchInts(s.idxSl, n)
	b := append(pkHash, rowHash...)
	if i == len(s.idxSl) {
		s.idxSl = append(s.idxSl, n)
		s.rowSl = append(s.rowSl, b)
	} else {
		s.idxSl = append(s.idxSl[:i+1], s.idxSl[i:]...)
		s.idxSl[i] = n
		s.rowSl = append(s.rowSl[:i+1], s.rowSl[i:]...)
		s.rowSl[i] = b
	}
	if i >= s.size && i+s.offset == n {
		err := s.flushRowListFile(i + 1)
		if err != nil {
			return err
		}
	}
	return s.db.Set(rowMapKey(s.id, pkHash), rowHash)
}

func (s *bigRowStore) Flush() error {
	// output the rest to row list file
	for _, b := range s.rowSl {
		_, err := s.rowListFile.Write(b)
		if err != nil {
			return err
		}
	}
	err := s.rowListFile.Close()
	if err != nil {
		return err
	}
	s.rowListFile = nil
	s.offset = 0
	s.idxSl = nil
	s.rowSl = nil
	return nil
}

func (s *bigRowStore) GetRowHash(pkHash []byte) (rowHash []byte, ok bool) {
	v, err := s.db.Get(rowMapKey(s.id, pkHash))
	if err != nil {
		return nil, false
	}
	return v, true
}

func (s *bigRowStore) NumRows() (int, error) {
	size, err := s.fs.Size(rowListKey(s.id))
	if err != nil {
		return 0, err
	}
	return int(size / 32), nil
}

func (s *bigRowStore) NewRowHashReader(offset, size int) (RowHashReader, error) {
	l, err := s.NumRows()
	if err != nil {
		return nil, err
	}
	offset, size = capSize(l, offset, size)
	r, err := s.fs.ReadSeeker(rowListKey(s.id))
	if err != nil {
		return nil, err
	}
	_, err = r.Seek(int64(offset*32), io.SeekStart)
	if err != nil {
		return nil, err
	}
	return &bigRowHashReader{r: r, size: size}, nil
}

func (s *bigRowStore) NewRowReader(offset, size int) (RowReader, error) {
	l, err := s.NumRows()
	if err != nil {
		return nil, err
	}
	offset, size = capSize(l, offset, size)
	r, err := s.fs.ReadSeeker(rowListKey(s.id))
	if err != nil {
		return nil, err
	}
	_, err = r.Seek(int64(offset*32), io.SeekStart)
	if err != nil {
		return nil, err
	}
	return &bigRowReader{r: r, size: size, db: s.db}, nil
}

func (s *bigRowStore) Delete() error {
	err := s.db.Clear(rowMapPrefix(s.id))
	if err != nil {
		return err
	}
	return s.fs.Delete(rowListKey(s.id))
}

func i32tob(val uint32) []byte {
	r := make([]byte, 4)
	for i := uint32(0); i < 4; i++ {
		r[i] = byte((val >> (8 * i)) & 0xff)
	}
	return r
}

func newRowStoreID() []byte {
	id := uuid.New().String()
	return i32tob(meow.Checksum32(0, []byte(id)))
}

func generateRowStoreID(db kv.DB) ([]byte, error) {
	maxAttempts := 100
	for i := 0; i < maxAttempts; i++ {
		b := newRowStoreID()
		if !db.Exist(rowListKey(b)) {
			return b, nil
		}
	}
	return nil, fmt.Errorf("%d collisions when attempting to generate row store id", maxAttempts)
}
