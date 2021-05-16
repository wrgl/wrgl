package kv

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/stretchr/testify/mock"
)

type MockWriteCloser struct {
	b *bytes.Buffer
}

func NewMockWriteCloser() *MockWriteCloser {
	return &MockWriteCloser{
		b: bytes.NewBuffer(nil),
	}
}

func (w *MockWriteCloser) Write(p []byte) (int, error) {
	return w.b.Write(p)
}

func (w *MockWriteCloser) Close() error {
	return nil
}

func (w *MockWriteCloser) String() string {
	return w.b.String()
}

type MockStore struct {
	mock.Mock

	store      map[string][]byte
	EnableMock bool
}

func (s *MockStore) Get(k []byte) ([]byte, error) {
	if s.EnableMock {
		args := s.Called(k)
		return args.Get(0).([]byte), args.Error(1)
	}
	v, ok := s.store[string(k)]
	if !ok {
		return nil, KeyNotFoundError
	}
	return v, nil
}

func (s *MockStore) Size(k []byte) (uint64, error) {
	if s.EnableMock {
		args := s.Called(k)
		return args.Get(0).(uint64), args.Error(1)
	}
	v, ok := s.store[string(k)]
	if !ok {
		return 0, KeyNotFoundError
	}
	return uint64(len(v)), nil
}

func (s *MockStore) Sizes(prefix []byte) (map[string]uint64, error) {
	if s.EnableMock {
		args := s.Called(prefix)
		return args.Get(0).(map[string]uint64), args.Error(1)
	}
	result := map[string]uint64{}
	for k, v := range s.store {
		if strings.HasPrefix(k, string(prefix)) {
			result[k] = uint64(len(v))
		}
	}
	return result, nil
}

func (s *MockStore) Set(k, v []byte) error {
	if s.EnableMock {
		args := s.Called(k, v)
		return args.Error(0)
	}
	b := make([]byte, len(v))
	copy(b, v)
	s.store[string(k)] = b
	return nil
}

func (s *MockStore) Move(a, b []byte) error {
	if s.EnableMock {
		args := s.Called(a, b)
		return args.Error(0)
	}
	s.store[string(b)] = s.store[string(a)]
	delete(s.store, string(a))
	return nil
}

func (s *MockStore) Exist(k []byte) bool {
	if s.EnableMock {
		args := s.Called(k)
		return args.Bool(0)
	}
	_, ok := s.store[string(k)]
	return ok
}

func (s *MockStore) Delete(k []byte) error {
	if s.EnableMock {
		args := s.Called(k)
		return args.Error(0)
	}
	delete(s.store, string(k))
	return nil
}

func (s *MockStore) Close() error {
	if s.EnableMock {
		args := s.Called()
		return args.Error(0)
	}
	return nil
}

func (s *MockStore) Clear(prefix []byte) error {
	if s.EnableMock {
		args := s.Called(prefix)
		return args.Error(0)
	}
	str := string(prefix)
	for k := range s.store {
		if strings.HasPrefix(k, str) {
			delete(s.store, k)
		}
	}
	return nil
}

func (s *MockStore) BatchGet(keys [][]byte) ([][]byte, error) {
	if s.EnableMock {
		args := s.Called(keys)
		return args.Get(0).([][]byte), args.Error(1)
	}
	result := [][]byte{}
	for _, k := range keys {
		v, ok := s.store[string(k)]
		if !ok {
			return nil, KeyNotFoundError
		}
		result = append(result, v)
	}
	return result, nil
}

type MockFile struct {
	b   []byte
	off int
}

func NewMockFile(b []byte) *MockFile {
	return &MockFile{b: b}
}

func (f *MockFile) Write(p []byte) (n int, err error) {
	n, err = f.WriteAt(p, int64(f.off))
	f.off += len(p)
	return
}

func (f *MockFile) WriteAt(p []byte, off int64) (n int, err error) {
	n = len(p)
	total := n + int(off)
	if total > cap(f.b) {
		sl := make([]byte, total)
		copy(sl, f.b)
		f.b = sl
	}
	copy(f.b[off:], p)
	return
}

func (f *MockFile) Bytes() []byte {
	return f.b
}

func (f *MockFile) Read(p []byte) (n int, err error) {
	if f.off >= len(f.b) {
		return 0, io.EOF
	}
	n = copy(p, f.b[f.off:])
	f.off += n
	return
}

func (f *MockFile) Close() error {
	return nil
}

func (f *MockFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	default:
		return 0, errors.New("Seek: invalid whence")
	case io.SeekStart:
		break
	case io.SeekCurrent:
		offset += int64(f.off)
	case io.SeekEnd:
		offset += int64(len(f.b))
	}
	if offset < 0 {
		return 0, errors.New("Seek: invalid offset")
	}
	f.off = int(offset)
	return offset, nil
}

func (f *MockFile) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || off >= int64(len(f.b)) {
		return 0, io.EOF
	}
	n = copy(p, f.b[off:])
	if max := len(f.b) - int(off); len(p) > max {
		return n, io.EOF
	}
	return n, nil
}

func (s *MockStore) Reader(k []byte) (File, error) {
	if s.EnableMock {
		args := s.Called(k)
		return args.Get(0).(File), args.Error(1)
	}
	v, ok := s.store[string(k)]
	if !ok {
		return nil, KeyNotFoundError
	}
	return &MockFile{
		b: v,
	}, nil
}

type mockStoreWriter struct {
	s   *MockStore
	buf *bytes.Buffer
	key string
}

func (w *mockStoreWriter) Write(b []byte) (int, error) {
	return w.buf.Write(b)
}

func (w *mockStoreWriter) Close() error {
	w.s.store[w.key] = w.buf.Bytes()
	return nil
}

func (s *MockStore) Writer(k []byte) (io.WriteCloser, error) {
	return &mockStoreWriter{
		s:   s,
		key: string(k),
		buf: bytes.NewBufferString(""),
	}, nil
}

func (s *MockStore) AppendWriter(k []byte) (io.WriteCloser, error) {
	return &mockStoreWriter{
		s:   s,
		key: string(k),
		buf: bytes.NewBuffer(s.store[string(k)]),
	}, nil
}

func (s *MockStore) BatchSet(data map[string][]byte) error {
	if s.EnableMock {
		args := s.Called(data)
		return args.Error(0)
	}
	for k, v := range data {
		b := make([]byte, len(v))
		copy(b, v)
		s.store[string(k)] = b
	}
	return nil
}

func (s *MockStore) BatchExist(keys [][]byte) ([]bool, error) {
	if s.EnableMock {
		args := s.Called(keys)
		return args.Get(0).([]bool), args.Error(1)
	}
	result := []bool{}
	for _, k := range keys {
		_, ok := s.store[string(k)]
		result = append(result, ok)
	}
	return result, nil
}

func (s *MockStore) Filter(prefix []byte) (map[string][]byte, error) {
	if s.EnableMock {
		args := s.Called(prefix)
		return args.Get(0).(map[string][]byte), args.Error(1)
	}
	result := map[string][]byte{}
	for k, v := range s.store {
		if strings.HasPrefix(k, string(prefix)) {
			result[k] = v
		}
	}
	return result, nil
}

func (s *MockStore) FilterKey(prefix []byte) ([][]byte, error) {
	if s.EnableMock {
		args := s.Called(prefix)
		return args.Get(0).([][]byte), args.Error(1)
	}
	result := [][]byte{}
	for k := range s.store {
		if strings.HasPrefix(k, string(prefix)) {
			result = append(result, []byte(k))
		}
	}
	return result, nil
}

func (s *MockStore) NewTransaction() Txn {
	return s
}

func (s *MockStore) PartialCommit() error {
	if s.EnableMock {
		args := s.Called()
		return args.Error(0)
	}
	return nil
}

func (s *MockStore) Commit() error {
	if s.EnableMock {
		args := s.Called()
		return args.Error(0)
	}
	return nil
}

func (s *MockStore) Discard() {
	if s.EnableMock {
		s.Called()
	}
}

func (s *MockStore) GarbageCollect(dur time.Duration) {
	if s.EnableMock {
		s.Called(dur)
	}
}

func NewMockStore(enableMock bool) *MockStore {
	return &MockStore{
		store:      map[string][]byte{},
		EnableMock: enableMock,
	}
}
