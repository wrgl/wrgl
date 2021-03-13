package kv

import (
	"bytes"
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
	s.store[string(k)] = v
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

func (s *MockStore) ReadSeeker(k []byte) (io.ReadSeeker, error) {
	if s.EnableMock {
		args := s.Called(k)
		return args.Get(0).(io.ReadSeeker), args.Error(1)
	}
	v, ok := s.store[string(k)]
	if !ok {
		return nil, KeyNotFoundError
	}
	return bytes.NewReader(v), nil
}

func (s *MockStore) BatchSet(data map[string][]byte) error {
	if s.EnableMock {
		args := s.Called(data)
		return args.Error(0)
	}
	for k, v := range data {
		s.store[string(k)] = v
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

func (s *MockStore) FilterKey(prefix []byte) ([]string, error) {
	if s.EnableMock {
		args := s.Called(prefix)
		return args.Get(0).([]string), args.Error(1)
	}
	result := []string{}
	for k := range s.store {
		if strings.HasPrefix(k, string(prefix)) {
			result = append(result, k)
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
