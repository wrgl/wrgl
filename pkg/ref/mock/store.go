// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package refmock

import (
	"io"
	"strings"

	"github.com/wrgl/core/pkg/ref"
)

type mockReflogReader struct {
	logs []*ref.Reflog
	off  int
}

func (r *mockReflogReader) Read() (*ref.Reflog, error) {
	n := len(r.logs)
	if r.off >= n {
		return nil, io.EOF
	}
	rec := r.logs[n-1-r.off]
	r.off++
	return rec, nil
}

func (r *mockReflogReader) Close() error {
	return nil
}

type mockStore struct {
	refs map[string][]byte
	logs map[string][]*ref.Reflog
}

func NewStore() *mockStore {
	return &mockStore{
		refs: map[string][]byte{},
		logs: map[string][]*ref.Reflog{},
	}
}

func (s *mockStore) SetWithLog(key string, val []byte, log *ref.Reflog) error {
	s.Set(key, val)
	if v, ok := s.logs[key]; ok {
		s.logs[key] = append(v, log)
	} else {
		s.logs[key] = []*ref.Reflog{log}
	}
	return nil
}

func (s *mockStore) Set(key string, val []byte) error {
	s.refs[key] = make([]byte, len(val))
	copy(s.refs[key], val)
	return nil
}

func (s *mockStore) Get(key string) (val []byte, err error) {
	if v, ok := s.refs[key]; ok {
		return v, nil
	}
	return nil, ref.ErrKeyNotFound
}

func (s *mockStore) Delete(key string) error {
	delete(s.refs, key)
	delete(s.logs, key)
	return nil
}

func (s *mockStore) Filter(prefix string) (m map[string][]byte, err error) {
	m = map[string][]byte{}
	for k, v := range s.refs {
		if strings.HasPrefix(k, prefix) {
			m[k] = v
		}
	}
	return
}

func (s *mockStore) FilterKey(prefix string) (keys []string, err error) {
	for k := range s.refs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return
}

func (s *mockStore) Rename(oldKey, newKey string) (err error) {
	s.refs[newKey] = s.refs[oldKey]
	delete(s.refs, oldKey)
	if v, ok := s.logs[oldKey]; ok {
		s.logs[newKey] = v
		delete(s.logs, oldKey)
	}
	return nil
}

func (s *mockStore) Copy(srcKey, dstKey string) (err error) {
	s.refs[dstKey] = make([]byte, len(s.refs[srcKey]))
	copy(s.refs[dstKey], s.refs[srcKey])
	if v, ok := s.logs[srcKey]; ok {
		s.logs[dstKey] = v
	}
	return nil
}

func (s *mockStore) LogReader(key string) (ref.ReflogReader, error) {
	if v, ok := s.logs[key]; ok {
		return &mockReflogReader{logs: v}, nil
	}
	return nil, ref.ErrKeyNotFound
}
