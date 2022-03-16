// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objects

import (
	"bytes"
	"sort"

	"github.com/google/uuid"
	"github.com/klauspost/compress/s2"
	"github.com/pckhoi/meow"
)

var (
	blkPrefix         = []byte("blk/")
	tblPrefix         = []byte("tbl/")
	blkIdxPrefix      = []byte("blkidx/")
	tblIdxPrefix      = []byte("tblidx/")
	comPrefix         = []byte("com/")
	tblSumPrefix      = []byte("tblsum/")
	transactionPrefix = []byte("transaction/")
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

func tableProfileKey(sum []byte) []byte {
	return append(tblSumPrefix, sum...)
}

func transactionKey(id uuid.UUID) []byte {
	return append(transactionPrefix, id[:]...)
}

func saveObj(s Store, k, v []byte) (err error) {
	b := make([]byte, len(v))
	copy(b, v)
	return s.Set(k, b)
}

func SaveBlock(s Store, buf, content []byte) (sum, dst []byte, err error) {
	dst = s2.EncodeBetter(buf, content)
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

func SaveBlockIndex(s Store, buf, content []byte) (sum, dst []byte, err error) {
	arr := meow.Checksum(0, content)
	dst = s2.EncodeBetter(buf, content)
	err = saveObj(s, blockIndexKey(arr[:]), dst)
	if err != nil {
		return
	}
	return arr[:], dst, nil
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

func SaveTableProfile(s Store, sum, content []byte) (err error) {
	return saveObj(s, tableProfileKey(sum), content)
}

func SaveTransaction(s Store, id uuid.UUID, tx *Transaction) (err error) {
	buf := bytes.NewBuffer(nil)
	if _, err = tx.WriteTo(buf); err != nil {
		return err
	}
	return saveObj(s, transactionKey(id), buf.Bytes())
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

func GetBlockIndex(s Store, buf, sum []byte) (idx *BlockIndex, dst []byte, err error) {
	b, err := s.Get(blockIndexKey(sum))
	if err != nil {
		return
	}
	dst, err = s2.Decode(buf, b)
	if err != nil {
		return
	}
	_, idx, err = ReadBlockIndex(bytes.NewReader(dst))
	return idx, dst, err
}

func GetTable(s Store, sum []byte) (*Table, error) {
	b, err := s.Get(tableKey(sum))
	if err != nil {
		return nil, err
	}
	_, tbl, err := ReadTableFrom(bytes.NewReader(b))
	tbl.Sum = sum
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

func GetTableProfile(s Store, sum []byte) (*TableProfile, error) {
	b, err := s.Get(tableProfileKey(sum))
	if err != nil {
		return nil, err
	}
	ts := &TableProfile{}
	_, err = ts.ReadFrom(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	return ts, nil
}

func GetCommit(s Store, sum []byte) (*Commit, error) {
	b, err := s.Get(commitKey(sum))
	if err != nil {
		return nil, err
	}
	_, com, err := ReadCommitFrom(bytes.NewReader(b))
	com.Sum = sum
	return com, err
}

func GetTransaction(s Store, id uuid.UUID) (*Transaction, error) {
	b, err := s.Get(transactionKey(id))
	if err != nil {
		return nil, err
	}
	_, tx, err := ReadTransactionFrom(bytes.NewReader(b))
	return tx, err
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

func DeleteTableProfile(s Store, sum []byte) error {
	return s.Delete(tableProfileKey(sum))
}

func DeleteCommit(s Store, sum []byte) error {
	return s.Delete(commitKey(sum))
}

func DeleteTransaction(s Store, id uuid.UUID) error {
	return s.Delete(transactionKey(id))
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

func TransactionExist(s Store, id uuid.UUID) bool {
	return s.Exist(transactionKey(id))
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

func GetAllBlockIndexKeys(s Store) ([][]byte, error) {
	return getAllKeys(s, blkIdxPrefix)
}

func GetAllTableKeys(s Store) ([][]byte, error) {
	return getAllKeys(s, tblPrefix)
}

func GetAllTableIndexKeys(s Store) ([][]byte, error) {
	return getAllKeys(s, tblIdxPrefix)
}

func GetAllTableProfileKeys(s Store) ([][]byte, error) {
	return getAllKeys(s, tblSumPrefix)
}

func GetAllCommitKeys(s Store) ([][]byte, error) {
	return getAllKeys(s, comPrefix)
}

func GetAllTransactionKeys(s Store) (ids []uuid.UUID, err error) {
	keys, err := getAllKeys(s, transactionPrefix)
	if err != nil {
		return nil, err
	}
	ids = make([]uuid.UUID, len(keys))
	for i, k := range keys {
		ids[i], err = uuid.FromBytes(k)
		if err != nil {
			return nil, err
		}
	}
	return ids, nil
}

func DeleteAllCommit(s Store) error {
	return s.Clear(comPrefix)
}
