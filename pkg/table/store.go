// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package table

type Store interface {
	Columns() []string
	PrimaryKey() []string
	PrimaryKeyIndices() []uint32
	GetRowHash(pkHash []byte) (rowHash []byte, ok bool)
	NumRows() int
	NewRowHashReader(offset, size int) RowHashReader
	NewRowReader() RowReader
	Close() error
}
