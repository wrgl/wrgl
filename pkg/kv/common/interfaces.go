// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package kvcommon

import (
	"io"
	"time"
)

type DB interface {
	Get([]byte) ([]byte, error)
	Set([]byte, []byte) error
	Delete([]byte) error
	Exist([]byte) bool
	Filter([]byte) (map[string][]byte, error)
	FilterKey([]byte) ([][]byte, error)
}

type Store interface {
	DB
	BatchGet([][]byte) ([][]byte, error)
	BatchSet(map[string][]byte) error
	Clear([]byte) error
	BatchExist(keys [][]byte) ([]bool, error)
	NewTransaction() Txn
	GarbageCollect(dur time.Duration)
	Close() error
}

type Txn interface {
	DB
	PartialCommit() error
	Commit() error
	Discard()
}

type File interface {
	io.ReadSeekCloser
	io.ReaderAt
	io.Writer
}
