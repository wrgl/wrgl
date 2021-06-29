// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kvcommon "github.com/wrgl/core/pkg/kv/common"
	kvtestutils "github.com/wrgl/core/pkg/kv/testutils"
	"github.com/wrgl/core/pkg/testutils"
)

func TestSaveObjects(t *testing.T) {
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

	sum = testutils.SecureRandomBytes(16)
	content = testutils.SecureRandomBytes(500)
	require.NoError(t, SaveBlockIndex(db, sum, content))
	b, err = GetBlockIndex(db, sum)
	require.NoError(t, err)
	assert.Equal(t, content, b)
	require.NoError(t, DeleteBlockIndex(db, sum))
	_, err = GetBlock(db, sum)
	assert.Equal(t, kvcommon.ErrKeyNotFound, err)

	sum = testutils.SecureRandomBytes(16)
	content = testutils.SecureRandomBytes(500)
	require.NoError(t, SaveTable(db, sum, content))
	b, err = GetTable(db, sum)
	require.NoError(t, err)
	assert.Equal(t, content, b)
	require.NoError(t, DeleteTable(db, sum))
	_, err = GetBlock(db, sum)
	assert.Equal(t, kvcommon.ErrKeyNotFound, err)

	sum = testutils.SecureRandomBytes(16)
	content = testutils.SecureRandomBytes(500)
	require.NoError(t, SaveTableIndex(db, sum, content))
	b, err = GetTableIndex(db, sum)
	require.NoError(t, err)
	assert.Equal(t, content, b)
	require.NoError(t, DeleteTableIndex(db, sum))
	_, err = GetBlock(db, sum)
	assert.Equal(t, kvcommon.ErrKeyNotFound, err)

	sum = testutils.SecureRandomBytes(16)
	content = testutils.SecureRandomBytes(500)
	require.NoError(t, SaveCommit(db, sum, content))
	b, err = GetCommit(db, sum)
	require.NoError(t, err)
	assert.Equal(t, content, b)
	require.NoError(t, DeleteCommit(db, sum))
	_, err = GetBlock(db, sum)
	assert.Equal(t, kvcommon.ErrKeyNotFound, err)
}
