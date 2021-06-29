// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package kv

import kvcommon "github.com/wrgl/core/pkg/kv/common"

func blockKey(sum []byte) []byte {
	return append([]byte("blk/"), sum...)
}

func tableKey(sum []byte) []byte {
	return append([]byte("tbl/"), sum...)
}

func blockIndexKey(sum []byte) []byte {
	return append([]byte("blkidx/"), sum...)
}

func tableIndexKey(sum []byte) []byte {
	return append([]byte("tblidx/"), sum...)
}

func commitKey(sum []byte) []byte {
	return append([]byte("com/"), sum...)
}

func SaveBlock(db kvcommon.DB, sum, content []byte) error {
	return db.Set(blockKey(sum), content)
}

func SaveBlockIndex(db kvcommon.DB, sum, content []byte) error {
	return db.Set(blockIndexKey(sum), content)
}

func SaveTable(db kvcommon.DB, sum, content []byte) error {
	return db.Set(tableKey(sum), content)
}

func SaveTableIndex(db kvcommon.DB, sum, content []byte) error {
	return db.Set(tableIndexKey(sum), content)
}

func SaveCommit(db kvcommon.DB, sum, content []byte) error {
	return db.Set(commitKey(sum), content)
}

func GetBlock(db kvcommon.DB, sum []byte) ([]byte, error) {
	return db.Get(blockKey(sum))
}

func GetBlockIndex(db kvcommon.DB, sum []byte) ([]byte, error) {
	return db.Get(blockIndexKey(sum))
}

func GetTable(db kvcommon.DB, sum []byte) ([]byte, error) {
	return db.Get(tableKey(sum))
}

func GetTableIndex(db kvcommon.DB, sum []byte) ([]byte, error) {
	return db.Get(tableIndexKey(sum))
}

func GetCommit(db kvcommon.DB, sum []byte) ([]byte, error) {
	return db.Get(commitKey(sum))
}

func DeleteBlock(db kvcommon.DB, sum []byte) error {
	return db.Delete(blockKey(sum))
}

func DeleteBlockIndex(db kvcommon.DB, sum []byte) error {
	return db.Delete(blockIndexKey(sum))
}

func DeleteTable(db kvcommon.DB, sum []byte) error {
	return db.Delete(tableKey(sum))
}

func DeleteTableIndex(db kvcommon.DB, sum []byte) error {
	return db.Delete(tableIndexKey(sum))
}

func DeleteCommit(db kvcommon.DB, sum []byte) error {
	return db.Delete(commitKey(sum))
}
