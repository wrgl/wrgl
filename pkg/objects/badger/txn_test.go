// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objbadger

import (
	"os"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestBadgerTxn(t *testing.T) {
	dir, err := testutils.TempDir("", "badger")
	require.NoError(t, err)
	db, err := badger.Open(badger.DefaultOptions(dir).WithLoggingLevel(badger.ERROR))
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
		err = os.RemoveAll(dir)
		require.NoError(t, err)
	}()

	s := NewStore(db)
	txn := NewTxn(db)
	defer txn.Discard()
	err = txn.Set([]byte("a"), []byte("b"))
	require.NoError(t, err)
	err = txn.Set([]byte("pdfs/1"), []byte("1"))
	require.NoError(t, err)
	require.NoError(t, txn.PartialCommit())
	err = txn.Set([]byte("pdfs/2"), []byte("2"))
	require.NoError(t, err)
	err = txn.Commit()
	require.NoError(t, err)

	v, err := s.Get([]byte("a"))
	require.NoError(t, err)
	assert.Equal(t, []byte("b"), v)

	txn = NewTxn(db)
	defer txn.Discard()
	assert.True(t, txn.Exist([]byte("a")))
	assert.False(t, txn.Exist([]byte("c")))

	m, err := txn.Filter([]byte("pdfs/"))
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"pdfs/1": []byte("1"),
		"pdfs/2": []byte("2"),
	}, m)

	sl, err := txn.FilterKey([]byte("pdfs/"))
	require.NoError(t, err)
	sort.Slice(sl, func(i, j int) bool { return string(sl[i]) < string(sl[j]) })
	assert.Equal(t, [][]byte{[]byte("pdfs/1"), []byte("pdfs/2")}, sl)

	v, err = txn.Get([]byte("a"))
	require.NoError(t, err)
	assert.Equal(t, []byte("b"), v)
	_, err = txn.Get([]byte("c"))
	assert.Equal(t, objects.ErrKeyNotFound, err)

	err = txn.Delete([]byte("a"))
	require.NoError(t, err)

	err = txn.Set([]byte("c"), []byte("d"))
	require.NoError(t, err)
	err = txn.Commit()
	require.NoError(t, err)

	_, err = s.Get([]byte("a"))
	assert.Equal(t, objects.ErrKeyNotFound, err)
	v, err = s.Get([]byte("c"))
	require.NoError(t, err)
	assert.Equal(t, []byte("d"), v)

	txn = NewTxn(db)
	defer txn.Discard()
	err = txn.Set([]byte("e"), []byte("f"))
	require.NoError(t, err)
	err = txn.Delete([]byte("c"))
	require.NoError(t, err)
	txn.Discard()

	_, err = s.Get([]byte("e"))
	assert.Equal(t, objects.ErrKeyNotFound, err)
	v, err = s.Get([]byte("c"))
	require.NoError(t, err)
	assert.Equal(t, []byte("d"), v)
}
