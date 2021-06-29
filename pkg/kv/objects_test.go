// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package kv

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kvcommon "github.com/wrgl/core/pkg/kv/common"
	kvtestutils "github.com/wrgl/core/pkg/kv/testutils"
	"github.com/wrgl/core/pkg/testutils"
)

func TestSaveBlock(t *testing.T) {
	db := kvtestutils.NewMockStore(false)

	sum := testutils.SecureRandomBytes(16)
	content := testutils.SecureRandomBytes(500)
	require.NoError(t, SaveBlock(db, sum, content))
	b, err := GetBlock(db, sum)
	require.NoError(t, err)
	assert.Equal(t, content, b)
	require.NoError(t, DeleteBlock(db, sum))
	_, err = GetBlock(db, sum)
	assert.Equal(t, kvcommon.ErrKeyNotFound, err)
}

func TestSaveBlockIndex(t *testing.T) {
	db := kvtestutils.NewMockStore(false)

	sum := testutils.SecureRandomBytes(16)
	content := testutils.SecureRandomBytes(500)
	require.NoError(t, SaveBlockIndex(db, sum, content))
	b, err := GetBlockIndex(db, sum)
	require.NoError(t, err)
	assert.Equal(t, content, b)
	require.NoError(t, DeleteBlockIndex(db, sum))
	_, err = GetBlock(db, sum)
	assert.Equal(t, kvcommon.ErrKeyNotFound, err)
}

func TestSaveTable(t *testing.T) {
	db := kvtestutils.NewMockStore(false)

	sum := testutils.SecureRandomBytes(16)
	content := testutils.SecureRandomBytes(500)
	require.NoError(t, SaveTable(db, sum, content))
	b, err := GetTable(db, sum)
	require.NoError(t, err)
	assert.Equal(t, content, b)
	require.NoError(t, DeleteTable(db, sum))
	_, err = GetBlock(db, sum)
	assert.Equal(t, kvcommon.ErrKeyNotFound, err)
}

func TestSaveTableIndex(t *testing.T) {
	db := kvtestutils.NewMockStore(false)

	sum := testutils.SecureRandomBytes(16)
	content := testutils.SecureRandomBytes(500)
	require.NoError(t, SaveTableIndex(db, sum, content))
	b, err := GetTableIndex(db, sum)
	require.NoError(t, err)
	assert.Equal(t, content, b)
	require.NoError(t, DeleteTableIndex(db, sum))
	_, err = GetBlock(db, sum)
	assert.Equal(t, kvcommon.ErrKeyNotFound, err)
}

func TestSaveCommit(t *testing.T) {
	db := kvtestutils.NewMockStore(false)

	sum1 := testutils.SecureRandomBytes(16)
	content1 := testutils.SecureRandomBytes(500)
	require.NoError(t, SaveCommit(db, sum1, content1))
	b, err := GetCommit(db, sum1)
	require.NoError(t, err)
	assert.Equal(t, content1, b)

	sum2 := testutils.SecureRandomBytes(16)
	content2 := testutils.SecureRandomBytes(500)
	require.NoError(t, SaveCommit(db, sum2, content2))

	sl, err := GetAllCommits(db)
	require.NoError(t, err)
	orig := [][]byte{sum1, sum2}
	sort.Slice(orig, func(i, j int) bool {
		return string(orig[i]) < string(orig[j])
	})
	assert.Equal(t, orig, sl)

	require.NoError(t, DeleteCommit(db, sum1))
	_, err = GetBlock(db, sum1)
	assert.Equal(t, kvcommon.ErrKeyNotFound, err)

	assert.True(t, CommitExist(db, sum2))
	require.NoError(t, DeleteAllCommit(db))
	assert.False(t, CommitExist(db, sum2))
}
