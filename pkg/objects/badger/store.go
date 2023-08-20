// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objbadger

import (
	"errors"

	"github.com/dgraph-io/badger/v3"
	"github.com/wrgl/wrgl/pkg/objects"
)

type Store struct {
	db *badger.DB
}

func NewStore(db *badger.DB) *Store {
	return &Store{db: db}
}

func (s *Store) Get(k []byte) ([]byte, error) {
	var v []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return objects.ErrKeyNotFound
			}
			return err
		}
		return item.Value(func(val []byte) error {
			v = val
			return nil
		})
	})
	return v, err
}

func (s *Store) Set(k, v []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(k, v)
	})
}

func (s *Store) Delete(k []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})
}

func (s *Store) Exist(k []byte) bool {
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(k)
		return err
	})
	return err == nil
}

func (s *Store) Filter(prefix []byte) (map[string][]byte, error) {
	result := map[string][]byte{}
	err := s.db.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.Prefix = prefix
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := string(item.KeyCopy(nil))
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			result[k] = v
		}
		return nil
	})
	return result, err
}

func (s *Store) FilterKey(prefix []byte) (keys [][]byte, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.Prefix = prefix
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			keys = append(keys, item.KeyCopy(nil))
		}
		return nil
	})
	return
}

func (s *Store) Clear(prefix []byte) error {
	return s.db.DropPrefix(prefix)
}

func (s *Store) Close() error {
	return s.db.Close()
}
