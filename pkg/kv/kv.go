package kv

import "time"

type DB interface {
	Get([]byte) ([]byte, error)
	Set([]byte, []byte) error
	Delete([]byte) error
	Exist([]byte) bool
	Filter([]byte) (map[string][]byte, error)
	FilterKey([]byte) ([]string, error)
}

type Store interface {
	DB
	BatchGet([][]byte) ([][]byte, error)
	BatchSet(map[string][]byte) error
	Clear([]byte) error
	BatchExist(keys [][]byte) ([]bool, error)
	NewTransaction() Txn
	GarbageCollect(dur time.Duration)
}

type Txn interface {
	DB
	PartialCommit() error
	Commit() error
	Discard()
}
