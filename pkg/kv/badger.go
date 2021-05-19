// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package kv

import (
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/prometheus/client_golang/prometheus"
)

type BadgerStore struct {
	db *badger.DB
}

func NewBadgerStore(db *badger.DB) *BadgerStore {
	return &BadgerStore{db: db}
}

func (s *BadgerStore) Get(k []byte) ([]byte, error) {
	var v []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return KeyNotFoundError
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

func (s *BadgerStore) Set(k []byte, v []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(k, v)
	})
}

func (s *BadgerStore) Delete(k []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})
}

func (s *BadgerStore) Clear(prefix []byte) error {
	return s.db.DropPrefix(prefix)
}

func (s *BadgerStore) BatchGet(keys [][]byte) ([][]byte, error) {
	vals := [][]byte{}
	err := s.db.View(func(txn *badger.Txn) error {
		for _, k := range keys {
			item, err := txn.Get(k)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					return KeyNotFoundError
				}
				return err
			}
			err = item.Value(func(val []byte) error {
				vals = append(vals, val)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return vals, err
}

func (s *BadgerStore) BatchExist(keys [][]byte) ([]bool, error) {
	result := []bool{}
	err := s.db.View(func(txn *badger.Txn) error {
		for _, k := range keys {
			if len(k) <= 0 {
				result = append(result, false)
				continue
			}
			_, err := txn.Get(k)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					result = append(result, false)
					continue
				}
				return err
			}
			result = append(result, true)
		}
		return nil
	})
	return result, err
}

func (s *BadgerStore) Filter(prefix []byte) (map[string][]byte, error) {
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

func (s *BadgerStore) FilterKey(prefix []byte) (keys [][]byte, err error) {
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

func (s *BadgerStore) Exist(k []byte) bool {
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(k)
		return err
	})
	return err == nil
}

func (s *BadgerStore) BatchSet(data map[string][]byte) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()
	for k, v := range data {
		err := txn.Set([]byte(k), v)
		if err != nil {
			if err == badger.ErrTxnTooBig {
				err := txn.Commit()
				if err != nil {
					return err
				}
				txn = s.db.NewTransaction(true)
				err = txn.Set([]byte(k), v)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}
	err := txn.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (s *BadgerStore) GarbageCollect(dur time.Duration) {
	ticker := time.NewTicker(dur)
	defer ticker.Stop()
	for range ticker.C {
	again:
		err := s.db.RunValueLogGC(0.5)
		if err == nil {
			goto again
		}
	}
}

func (s *BadgerStore) Close() error {
	return s.db.Close()
}

func ExposeBadgerToPrometheus() error {
	badgerExpvarCollector := prometheus.NewExpvarCollector(map[string]*prometheus.Desc{
		"badger_v2_blocked_puts_total":   prometheus.NewDesc("badger_blocked_puts_total", "Blocked Puts", nil, nil),
		"badger_v2_disk_reads_total":     prometheus.NewDesc("badger_disk_reads_total", "Disk Reads", nil, nil),
		"badger_v2_disk_writes_total":    prometheus.NewDesc("badger_disk_writes_total", "Disk Writes", nil, nil),
		"badger_v2_gets_total":           prometheus.NewDesc("badger_gets_total", "Gets", nil, nil),
		"badger_v2_puts_total":           prometheus.NewDesc("badger_puts_total", "Puts", nil, nil),
		"badger_v2_memtable_gets_total":  prometheus.NewDesc("badger_memtable_gets_total", "Memtable gets", nil, nil),
		"badger_v2_lsm_size_bytes":       prometheus.NewDesc("badger_lsm_size_bytes", "LSM Size in bytes", []string{"database"}, nil),
		"badger_v2_vlog_size_bytes":      prometheus.NewDesc("badger_vlog_size_bytes", "Value Log Size in bytes", []string{"database"}, nil),
		"badger_v2_pending_writes_total": prometheus.NewDesc("badger_pending_writes_total", "Pending Writes", []string{"database"}, nil),
		"badger_v2_read_bytes":           prometheus.NewDesc("badger_read_bytes", "Read bytes", nil, nil),
		"badger_v2_written_bytes":        prometheus.NewDesc("badger_written_bytes", "Written bytes", nil, nil),
		"badger_v2_lsm_bloom_hits_total": prometheus.NewDesc("badger_lsm_bloom_hits_total", "LSM Bloom Hits", []string{"level"}, nil),
		"badger_v2_lsm_level_gets_total": prometheus.NewDesc("badger_lsm_level_gets_total", "LSM Level Gets", []string{"level"}, nil),
	})
	if err := prometheus.Register(badgerExpvarCollector); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			badgerExpvarCollector = are.ExistingCollector
		} else {
			return err
		}
	}
	return nil
}
