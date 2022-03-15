// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objmock

import (
	"strings"

	"github.com/wrgl/wrgl/pkg/objects"
)

type Store struct {
	m map[string][]byte
}

func NewStore() *Store {
	return &Store{
		m: map[string][]byte{},
	}
}

func (s *Store) Get(key []byte) ([]byte, error) {
	if v, ok := s.m[string(key)]; ok {
		return v, nil
	}
	return nil, objects.ErrKeyNotFound
}

func (s *Store) Set(key, val []byte) error {
	s.m[string(key)] = make([]byte, len(val))
	copy(s.m[string(key)], val)
	return nil
}

func (s *Store) Delete(key []byte) error {
	delete(s.m, string(key))
	return nil
}

func (s *Store) Exist(key []byte) bool {
	if _, ok := s.m[string(key)]; ok {
		return true
	}
	return false
}

func (s *Store) Filter(prefix []byte) (map[string][]byte, error) {
	m := map[string][]byte{}
	for k, v := range s.m {
		if strings.HasPrefix(k, string(prefix)) {
			m[k] = v
		}
	}
	return m, nil
}

func (s *Store) FilterKey(prefix []byte) ([][]byte, error) {
	keys := [][]byte{}
	for k := range s.m {
		if strings.HasPrefix(k, string(prefix)) {
			keys = append(keys, []byte(k))
		}
	}
	return keys, nil
}

func (s *Store) Clear(prefix []byte) error {
	for k := range s.m {
		if strings.HasPrefix(k, string(prefix)) {
			delete(s.m, k)
		}
	}
	return nil
}

func (s *Store) Close() error {
	return nil
}
