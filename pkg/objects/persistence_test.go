// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects_test

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/objects"
	objhelpers "github.com/wrgl/core/pkg/objects/helpers"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/testutils"
)

func TestSaveBlock(t *testing.T) {
	s := objmock.NewStore()

	blk := testutils.BuildRawCSV(5, 10)
	buf := bytes.NewBuffer(nil)
	_, err := objects.WriteBlockTo(buf, blk)
	require.NoError(t, err)
	sum, err := objects.SaveBlock(s, buf.Bytes())
	require.NoError(t, err)
	assert.True(t, objects.BlockExist(s, sum))
	obj, err := objects.GetBlock(s, sum)
	require.NoError(t, err)
	assert.Equal(t, blk, obj)
	require.NoError(t, objects.DeleteBlock(s, sum))
	assert.False(t, objects.BlockExist(s, sum))
	_, err = objects.GetBlock(s, sum)
	assert.Equal(t, objects.ErrKeyNotFound, err)
}

func TestSaveBlockIndex(t *testing.T) {
	s := objmock.NewStore()

	idx := objects.IndexBlock(testutils.BuildRawCSV(5, 10), []uint32{0})
	buf := bytes.NewBuffer(nil)
	_, err := idx.WriteTo(buf)
	require.NoError(t, err)
	sum := testutils.SecureRandomBytes(16)
	require.NoError(t, objects.SaveBlockIndex(s, sum, buf.Bytes()))
	assert.True(t, objects.BlockIndexExist(s, sum))
	obj, err := objects.GetBlockIndex(s, sum)
	require.NoError(t, err)
	assert.Equal(t, idx, obj)
	require.NoError(t, objects.DeleteBlockIndex(s, sum))
	assert.False(t, objects.BlockIndexExist(s, sum))
	_, err = objects.GetBlock(s, sum)
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
	}
	buf := bytes.NewBuffer(nil)
	_, err := tbl.WriteTo(buf)
	require.NoError(t, err)
	sum, err := objects.SaveTable(s, buf.Bytes())
	require.NoError(t, err)
	assert.True(t, objects.TableExist(s, sum))
	obj, err := objects.GetTable(s, sum)
	require.NoError(t, err)
	assert.Equal(t, tbl, obj)
	require.NoError(t, objects.DeleteTable(s, sum))
	assert.False(t, objects.TableExist(s, sum))
	_, err = objects.GetBlock(s, sum)
	assert.Equal(t, objects.ErrKeyNotFound, err)
}

func TestSaveTableIndex(t *testing.T) {
	s := objmock.NewStore()

	idx := testutils.BuildRawCSV(2, 10)[1:]
	buf := bytes.NewBuffer(nil)
	_, err := objects.WriteBlockTo(buf, idx)
	require.NoError(t, err)
	sum := testutils.SecureRandomBytes(16)
	require.NoError(t, objects.SaveTableIndex(s, sum, buf.Bytes()))
	assert.True(t, objects.TableIndexExist(s, sum))
	obj, err := objects.GetTableIndex(s, sum)
	require.NoError(t, err)
	assert.Equal(t, idx, obj)
	require.NoError(t, objects.DeleteTableIndex(s, sum))
	assert.False(t, objects.TableIndexExist(s, sum))
	_, err = objects.GetBlock(s, sum)
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

	sl, err := objects.GetAllCommits(s)
	require.NoError(t, err)
	orig := [][]byte{sum1, sum2}
	sort.Slice(orig, func(i, j int) bool {
		return string(orig[i]) < string(orig[j])
	})
	assert.Equal(t, orig, sl)

	require.NoError(t, objects.DeleteCommit(s, sum1))
	_, err = objects.GetBlock(s, sum1)
	assert.Equal(t, objects.ErrKeyNotFound, err)

	assert.True(t, objects.CommitExist(s, sum2))
	require.NoError(t, objects.DeleteAllCommit(s))
	assert.False(t, objects.CommitExist(s, sum2))
}
