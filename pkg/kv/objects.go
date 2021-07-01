// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package kv

import (
	"sort"

	kvcommon "github.com/wrgl/core/pkg/kv/common"
)

var (
	blkPrefix    = []byte("blk/")
	tblPrefix    = []byte("tbl/")
	blkIdxPrefix = []byte("blkidx/")
	tblIdxPrefix = []byte("tblidx/")
	comPrefix    = []byte("com/")
)

func blockKey(sum []byte) []byte {
	return append(blkPrefix, sum...)
}

func tableKey(sum []byte) []byte {
	return append(tblPrefix, sum...)
}

func blockIndexKey(sum []byte) []byte {
	return append(blkIdxPrefix, sum...)
}

func tableIndexKey(sum []byte) []byte {
	return append(tblIdxPrefix, sum...)
}

func commitKey(sum []byte) []byte {
	return append(comPrefix, sum...)
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

func BlockExist(s kvcommon.DB, sum []byte) bool {
	return s.Exist(blockKey(sum))
}

func BlockIndexExist(s kvcommon.DB, sum []byte) bool {
	return s.Exist(blockIndexKey(sum))
}

func TableExist(s kvcommon.DB, sum []byte) bool {
	return s.Exist(tableKey(sum))
}

func TableIndexExist(s kvcommon.DB, sum []byte) bool {
	return s.Exist(tableIndexKey(sum))
}

func CommitExist(s kvcommon.DB, sum []byte) bool {
	return s.Exist(commitKey(sum))
}

func GetAllCommits(s kvcommon.DB) ([][]byte, error) {
	sl, err := s.FilterKey(comPrefix)
	if err != nil {
		return nil, err
	}
	l := len(comPrefix)
	result := make([][]byte, len(sl))
	for i, h := range sl {
		result[i] = h[l:]
	}
	sort.Slice(result, func(i, j int) bool {
		return string(result[i]) < string(result[j])
	})
	return result, nil
}

func DeleteAllCommit(s kvcommon.Store) error {
	return s.Clear(comPrefix)
}
