// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"bytes"
	"sort"

	"github.com/klauspost/compress/s2"
	"github.com/mmcloughlin/meow"
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

func saveObj(s Store, k, v []byte) (err error) {
	b := make([]byte, len(v))
	copy(b, v)
	return s.Set(k, b)
}

func SaveBlock(s Store, buf, content []byte) (sum, dst []byte, err error) {
	dst = s2.Encode(buf, content)
	sum, err = SaveCompressedBlock(s, content, dst)
	return
}

func SaveCompressedBlock(s Store, content, compressed []byte) (sum []byte, err error) {
	sumArr := meow.Checksum(0, content)
	if err = saveObj(s, blockKey(sumArr[:]), compressed); err != nil {
		return
	}
	return sumArr[:], nil
}

func SaveBlockIndex(s Store, content []byte) (sum []byte, err error) {
	arr := meow.Checksum(0, content)
	err = saveObj(s, blockIndexKey(arr[:]), content)
	if err != nil {
		return
	}
	return arr[:], nil
}

func SaveTable(s Store, content []byte) (sum []byte, err error) {
	arr := meow.Checksum(0, content)
	err = saveObj(s, tableKey(arr[:]), content)
	if err != nil {
		return
	}
	return arr[:], nil
}

func SaveTableIndex(s Store, sum, content []byte) (err error) {
	return saveObj(s, tableIndexKey(sum), content)
}

func SaveCommit(s Store, content []byte) (sum []byte, err error) {
	arr := meow.Checksum(0, content)
	err = saveObj(s, commitKey(arr[:]), content)
	if err != nil {
		return
	}
	return arr[:], nil
}

func GetBlockBytes(s Store, sum []byte) ([]byte, error) {
	return s.Get(blockKey(sum))
}

func GetBlock(s Store, buf, sum []byte) (blk [][]string, dst []byte, err error) {
	b, err := GetBlockBytes(s, sum)
	if err != nil {
		return
	}
	dst, err = s2.Decode(buf, b)
	if err != nil {
		return
	}
	_, blk, err = ReadBlockFrom(bytes.NewReader(dst))
	return
}

func GetBlockIndex(s Store, sum []byte) (*BlockIndex, error) {
	b, err := s.Get(blockIndexKey(sum))
	if err != nil {
		return nil, err
	}
	_, idx, err := ReadBlockIndex(bytes.NewReader(b))
	return idx, err
}

func GetTable(s Store, sum []byte) (*Table, error) {
	b, err := s.Get(tableKey(sum))
	if err != nil {
		return nil, err
	}
	_, tbl, err := ReadTableFrom(bytes.NewReader(b))
	return tbl, err
}

func GetTableIndex(s Store, sum []byte) ([][]string, error) {
	b, err := s.Get(tableIndexKey(sum))
	if err != nil {
		return nil, err
	}
	_, idx, err := ReadBlockFrom(bytes.NewReader(b))
	return idx, err
}

func GetCommit(s Store, sum []byte) (*Commit, error) {
	b, err := s.Get(commitKey(sum))
	if err != nil {
		return nil, err
	}
	_, com, err := ReadCommitFrom(bytes.NewReader(b))
	return com, err
}

func DeleteBlock(s Store, sum []byte) error {
	return s.Delete(blockKey(sum))
}

func DeleteBlockIndex(s Store, sum []byte) error {
	return s.Delete(blockIndexKey(sum))
}

func DeleteTable(s Store, sum []byte) error {
	return s.Delete(tableKey(sum))
}

func DeleteTableIndex(s Store, sum []byte) error {
	return s.Delete(tableIndexKey(sum))
}

func DeleteCommit(s Store, sum []byte) error {
	return s.Delete(commitKey(sum))
}

func BlockExist(s Store, sum []byte) bool {
	return s.Exist(blockKey(sum))
}

func BlockIndexExist(s Store, sum []byte) bool {
	return s.Exist(blockIndexKey(sum))
}

func TableExist(s Store, sum []byte) bool {
	return s.Exist(tableKey(sum))
}

func TableIndexExist(s Store, sum []byte) bool {
	return s.Exist(tableIndexKey(sum))
}

func CommitExist(s Store, sum []byte) bool {
	return s.Exist(commitKey(sum))
}

func getAllKeys(s Store, prefix []byte) ([][]byte, error) {
	sl, err := s.FilterKey(prefix)
	if err != nil {
		return nil, err
	}
	l := len(prefix)
	result := make([][]byte, len(sl))
	for i, h := range sl {
		result[i] = h[l:]
	}
	sort.Slice(result, func(i, j int) bool {
		return string(result[i]) < string(result[j])
	})
	return result, nil
}

func GetAllBlockKeys(s Store) ([][]byte, error) {
	return getAllKeys(s, blkPrefix)
}

func GetAllTableKeys(s Store) ([][]byte, error) {
	return getAllKeys(s, tblPrefix)
}

func GetAllCommitKeys(s Store) ([][]byte, error) {
	return getAllKeys(s, comPrefix)
}

func DeleteAllCommit(s Store) error {
	return s.Clear(comPrefix)
}
