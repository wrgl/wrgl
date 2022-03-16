// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objects_test

import (
	"bytes"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pckhoi/meow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	objhelpers "github.com/wrgl/wrgl/pkg/objects/helpers"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestSaveBlock(t *testing.T) {
	s := objmock.NewStore()

	blk := testutils.BuildRawCSV(5, 10)
	buf := bytes.NewBuffer(nil)
	enc := objects.NewStrListEncoder(true)
	_, err := objects.WriteBlockTo(enc, buf, blk)
	require.NoError(t, err)
	bb := make([]byte, 1024)
	sum, bb, err := objects.SaveBlock(s, bb, buf.Bytes())
	require.NoError(t, err)
	assert.True(t, objects.BlockExist(s, sum))
	obj, bb, err := objects.GetBlock(s, bb, sum)
	require.NoError(t, err)
	assert.Equal(t, blk, obj)
	require.NoError(t, objects.DeleteBlock(s, sum))
	assert.False(t, objects.BlockExist(s, sum))
	_, _, err = objects.GetBlock(s, bb, sum)
	assert.Equal(t, objects.ErrKeyNotFound, err)
}

func TestSaveBlockIndex(t *testing.T) {
	s := objmock.NewStore()

	enc := objects.NewStrListEncoder(true)
	hash := meow.New(0)
	idx, err := objects.IndexBlock(enc, hash, testutils.BuildRawCSV(5, 10), []uint32{0})
	require.NoError(t, err)
	buf := bytes.NewBuffer(nil)
	_, err = idx.WriteTo(buf)
	require.NoError(t, err)
	sum, bb, err := objects.SaveBlockIndex(s, nil, buf.Bytes())
	require.NoError(t, err)
	assert.True(t, objects.BlockIndexExist(s, sum))
	obj, bb, err := objects.GetBlockIndex(s, bb, sum)
	require.NoError(t, err)
	assert.Equal(t, idx, obj)
	sl, err := objects.GetAllBlockIndexKeys(s)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{sum}, sl)
	require.NoError(t, objects.DeleteBlockIndex(s, sum))
	assert.False(t, objects.BlockIndexExist(s, sum))
	_, _, err = objects.GetBlock(s, bb, sum)
	assert.Equal(t, objects.ErrKeyNotFound, err)
}

func TestSaveTable(t *testing.T) {
	s := objmock.NewStore()

	tbl := &objects.Table{
		Columns:   []string{"a", "b", "c", "d"},
		PK:        []uint32{0},
		RowsCount: 700,
		Blocks: [][]byte{
			testutils.SecureRandomBytes(16),
			testutils.SecureRandomBytes(16),
			testutils.SecureRandomBytes(16),
		},
		BlockIndices: [][]byte{
			testutils.SecureRandomBytes(16),
			testutils.SecureRandomBytes(16),
			testutils.SecureRandomBytes(16),
		},
	}
	buf := bytes.NewBuffer(nil)
	_, err := tbl.WriteTo(buf)
	require.NoError(t, err)
	sum, err := objects.SaveTable(s, buf.Bytes())
	require.NoError(t, err)
	assert.True(t, objects.TableExist(s, sum))
	obj, err := objects.GetTable(s, sum)
	require.NoError(t, err)
	tbl.Sum = sum
	assert.Equal(t, tbl, obj)
	require.NoError(t, objects.DeleteTable(s, sum))
	assert.False(t, objects.TableExist(s, sum))
	bb := make([]byte, 1024)
	_, _, err = objects.GetBlock(s, bb, sum)
	assert.Equal(t, objects.ErrKeyNotFound, err)
}

func TestSaveTableIndex(t *testing.T) {
	s := objmock.NewStore()
	enc := objects.NewStrListEncoder(true)

	idx := testutils.BuildRawCSV(2, 10)[1:]
	buf := bytes.NewBuffer(nil)
	_, err := objects.WriteBlockTo(enc, buf, idx)
	require.NoError(t, err)
	sum := testutils.SecureRandomBytes(16)
	require.NoError(t, objects.SaveTableIndex(s, sum, buf.Bytes()))
	assert.True(t, objects.TableIndexExist(s, sum))
	obj, err := objects.GetTableIndex(s, sum)
	require.NoError(t, err)
	assert.Equal(t, idx, obj)
	sl, err := objects.GetAllTableIndexKeys(s)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{sum}, sl)
	require.NoError(t, objects.DeleteTableIndex(s, sum))
	assert.False(t, objects.TableIndexExist(s, sum))
	bb := make([]byte, 1024)
	_, _, err = objects.GetBlock(s, bb, sum)
	assert.Equal(t, objects.ErrKeyNotFound, err)
}

func floatPtr(f float64) *float64 {
	return &f
}

func TestSaveTableSummary(t *testing.T) {
	s := objmock.NewStore()
	tbl := &objects.TableProfile{
		RowsCount: 200,
		Columns: []*objects.ColumnProfile{
			{
				Name:         "a",
				NACount:      0,
				Min:          floatPtr(0),
				Max:          floatPtr(200),
				Mean:         floatPtr(3.123),
				Median:       floatPtr(5),
				StdDeviation: floatPtr(3.4),
				Percentiles: []float64{
					3, 7, 10, 14.69, 17, 21.69, 24, 28.69, 31, 34, 38, 41, 45, 48, 52.69, 55, 59.69, 62, 66.69,
				},
				MinStrLen: 1,
				MaxStrLen: 5,
				AvgStrLen: 2,
			},
			{
				Name:      "def",
				NACount:   20,
				MinStrLen: 10,
				MaxStrLen: 10,
				AvgStrLen: 10,
				TopValues: objects.ValueCounts{
					{testutils.BrokenRandomLowerAlphaString(10), 50},
					{testutils.BrokenRandomLowerAlphaString(10), 30},
					{testutils.BrokenRandomLowerAlphaString(10), 20},
					{testutils.BrokenRandomLowerAlphaString(10), 10},
				},
			},
		},
	}

	w := bytes.NewBuffer(nil)
	_, err := tbl.WriteTo(w)
	require.NoError(t, err)
	sum := testutils.SecureRandomBytes(16)
	require.NoError(t, objects.SaveTableProfile(s, sum, w.Bytes()))
	ts, err := objects.GetTableProfile(s, sum)
	require.NoError(t, err)
	assert.Equal(t, tbl, ts)
	sl, err := objects.GetAllTableProfileKeys(s)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{sum}, sl)
	require.NoError(t, objects.DeleteTableProfile(s, sum))
	_, err = objects.GetTableProfile(s, sum)
	assert.Equal(t, objects.ErrKeyNotFound, err)
}

func TestSaveCommit(t *testing.T) {
	s := objmock.NewStore()

	com1 := objhelpers.RandomCommit()
	buf := bytes.NewBuffer(nil)
	_, err := com1.WriteTo(buf)
	require.NoError(t, err)
	sum1, err := objects.SaveCommit(s, buf.Bytes())
	require.NoError(t, err)
	obj, err := objects.GetCommit(s, sum1)
	require.NoError(t, err)
	objhelpers.AssertCommitEqual(t, com1, obj)

	com2 := objhelpers.RandomCommit()
	buf.Reset()
	_, err = com2.WriteTo(buf)
	require.NoError(t, err)
	sum2, err := objects.SaveCommit(s, buf.Bytes())
	require.NoError(t, err)

	sl, err := objects.GetAllCommitKeys(s)
	require.NoError(t, err)
	orig := [][]byte{sum1, sum2}
	sort.Slice(orig, func(i, j int) bool {
		return string(orig[i]) < string(orig[j])
	})
	assert.Equal(t, orig, sl)

	require.NoError(t, objects.DeleteCommit(s, sum1))
	bb := make([]byte, 1024)
	_, _, err = objects.GetBlock(s, bb, sum1)
	assert.Equal(t, objects.ErrKeyNotFound, err)

	assert.True(t, objects.CommitExist(s, sum2))
	require.NoError(t, objects.DeleteAllCommit(s))
	assert.False(t, objects.CommitExist(s, sum2))
}

func TestSaveTransaction(t *testing.T) {
	s := objmock.NewStore()
	id1 := uuid.New()
	tx1 := &objects.Transaction{
		Begin: time.Now(),
	}
	id2 := uuid.New()
	tx2 := &objects.Transaction{
		Begin: time.Now().Add(-1 * time.Hour),
	}

	require.NoError(t, objects.SaveTransaction(s, id1, tx1))
	require.NoError(t, objects.SaveTransaction(s, id2, tx2))

	tx, err := objects.GetTransaction(s, id1)
	require.NoError(t, err)
	objhelpers.AssertTransactionEqual(t, tx1, tx)
	_, err = objects.GetTransaction(s, uuid.New())
	assert.Equal(t, objects.ErrKeyNotFound, err)

	keys, err := objects.GetAllTransactionKeys(s)
	require.NoError(t, err)
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) == -1 })
	ids := []uuid.UUID{id1, id2}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) == -1 })
	assert.Equal(t, ids, keys)

	require.NoError(t, objects.DeleteTransaction(s, id1))
	assert.False(t, objects.TransactionExist(s, id1))
	assert.True(t, objects.TransactionExist(s, id2))
}
