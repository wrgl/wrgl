// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objbadger

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/wrgl/wrgl/pkg/objects"
)

type Txn struct {
	db  *badger.DB
	txn *badger.Txn
}

func NewTxn(db *badger.DB) *Txn {
	return &Txn{
		db:  db,
		txn: db.NewTransaction(true),
	}
}

func (t *Txn) Get(k []byte) ([]byte, error) {
	item, err := t.txn.Get(k)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, objects.ErrKeyNotFound
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

func (t *Txn) Set(k, v []byte) error {
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

func (t *Txn) Delete(k []byte) error {
	return t.txn.Delete(k)
}

func (t *Txn) Exist(k []byte) bool {
	_, err := t.txn.Get(k)
	return err == nil
}

func (t *Txn) PartialCommit() error {
	err := t.txn.Commit()
	if err != nil {
		return err
	}
	t.txn = t.db.NewTransaction(true)
	return nil
}

func (t *Txn) Commit() error {
	return t.txn.Commit()
}

func (t *Txn) Discard() {
	t.txn.Discard()
}

func (t *Txn) Filter(prefix []byte) (map[string][]byte, error) {
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

func (t *Txn) FilterKey(prefix []byte) (keys [][]byte, err error) {
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

func (t *Txn) Clear([]byte) error {
	return fmt.Errorf("not implemented")
}

func (t *Txn) Close() error {
	t.txn.Discard()
	return t.db.Close()
}
