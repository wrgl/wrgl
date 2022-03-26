// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ref

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type ReflogReader interface {
	Read() (*Reflog, error)
	Close() error
}

type Store interface {
	SetWithLog(key string, val []byte, log *Reflog) error
	Set(key string, val []byte) error
	Get(key string) (val []byte, err error)
	Delete(key string) error
	Filter(prefixes, notPrefixes []string) (m map[string][]byte, err error)
	FilterKey(prefixes, notPrefixes []string) (keys []string, err error)
	Rename(oldKey, newKey string) (err error)
	Copy(srcKey, dstKey string) (err error)
	LogReader(key string) (ReflogReader, error)

	NewTransaction(tx *Transaction) (*uuid.UUID, error)
	GetTransaction(id uuid.UUID) (*Transaction, error)
	UpdateTransaction(tx *Transaction) error
	DeleteTransaction(id uuid.UUID) error
	GCTransactions(txTTL time.Duration) (ids []uuid.UUID, err error)
	GetTransactionLogs(txid uuid.UUID) (logs map[string]*Reflog, err error)
	ListTransactions(offset, limit int) (txs []*Transaction, err error)
}

var ErrKeyNotFound = fmt.Errorf("key not found")
