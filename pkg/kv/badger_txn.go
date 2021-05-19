// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package kv

import "github.com/dgraph-io/badger/v3"

type badgerTxn struct {
	db  *badger.DB
	txn *badger.Txn
}

func newBadgerTxn(db *badger.DB) *badgerTxn {
	return &badgerTxn{
		db:  db,
		txn: db.NewTransaction(true),
	}
}

func (t *badgerTxn) Get(k []byte) ([]byte, error) {
	item, err := t.txn.Get(k)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, KeyNotFoundError
		}
		return nil, err
	}
	var v []byte
	err = item.Value(func(val []byte) error {
		v = val
		return nil
	})
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (t *badgerTxn) Set(k, v []byte) error {
	err := t.txn.Set([]byte(k), v)
	if err != nil {
		if err == badger.ErrTxnTooBig {
			err := t.txn.Commit()
			if err != nil {
				return err
			}
			t.txn = t.db.NewTransaction(true)
			err = t.txn.Set([]byte(k), v)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func (t *badgerTxn) Delete(k []byte) error {
	return t.txn.Delete(k)
}

func (t *badgerTxn) Exist(k []byte) bool {
	_, err := t.txn.Get(k)
	return err == nil
}

func (t *badgerTxn) PartialCommit() error {
	err := t.txn.Commit()
	if err != nil {
		return err
	}
	t.txn = t.db.NewTransaction(true)
	return nil
}

func (t *badgerTxn) Commit() error {
	return t.txn.Commit()
}

func (t *badgerTxn) Discard() {
	t.txn.Discard()
}

func (s *BadgerStore) NewTransaction() Txn {
	return newBadgerTxn(s.db)
}

func (t *badgerTxn) Filter(prefix []byte) (map[string][]byte, error) {
	result := map[string][]byte{}
	opt := badger.DefaultIteratorOptions
	opt.Prefix = prefix
	it := t.txn.NewIterator(opt)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := string(item.KeyCopy(nil))
		v, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		result[k] = v
	}
	return result, nil
}

func (t *badgerTxn) FilterKey(prefix []byte) (keys [][]byte, err error) {
	opt := badger.DefaultIteratorOptions
	opt.Prefix = prefix
	it := t.txn.NewIterator(opt)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		keys = append(keys, item.KeyCopy(nil))
	}
	return
}
