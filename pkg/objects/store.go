// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objects

import "fmt"

type Store interface {
	Get([]byte) ([]byte, error)
	Set([]byte, []byte) error
	Delete([]byte) error
	Exist([]byte) bool
	Filter([]byte) (map[string][]byte, error)
	FilterKey([]byte) ([][]byte, error)
	Clear([]byte) error
	Close() error
}

var ErrKeyNotFound = fmt.Errorf("key not found")
