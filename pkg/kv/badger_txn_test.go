package kv

import (
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadgerTxn(t *testing.T) {
	badgerPath := "/tmp/badger"
	db, err := badger.Open(badger.DefaultOptions(badgerPath))
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
		err = os.RemoveAll(badgerPath)
		require.NoError(t, err)
	}()
	s := NewBadgerStore(db)

	err = s.RunInTransaction(func(txn Txn) error {
		err := txn.Set([]byte("a"), []byte("b"))
		require.NoError(t, err)
		err = txn.Set([]byte("pdfs/1"), []byte("1"))
		require.NoError(t, err)
		err = txn.Set([]byte("pdfs/2"), []byte("2"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	v, err := s.Get([]byte("a"))
	require.NoError(t, err)
	assert.Equal(t, []byte("b"), v)

	err = s.RunInTransaction(func(txn Txn) error {
		assert.True(t, txn.Exist([]byte("a")))
		assert.False(t, txn.Exist([]byte("c")))

		m, err := txn.Filter([]byte("pdfs/"))
		require.NoError(t, err)
		assert.Equal(t, map[string][]byte{
			"pdfs/1": []byte("1"),
			"pdfs/2": []byte("2"),
		}, m)

		sl, err := s.FilterKey([]byte("pdfs/"))
		require.NoError(t, err)
		sort.Slice(sl, func(i, j int) bool { return sl[i] < sl[j] })
		assert.Equal(t, []string{"pdfs/1", "pdfs/2"}, sl)

		v, err := txn.Get([]byte("a"))
		require.NoError(t, err)
		assert.Equal(t, []byte("b"), v)
		_, err = txn.Get([]byte("c"))
		assert.Equal(t, KeyNotFoundError, err)

		err = txn.Delete([]byte("a"))
		require.NoError(t, err)

		err = txn.Set([]byte("c"), []byte("d"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	_, err = s.Get([]byte("a"))
	assert.Equal(t, KeyNotFoundError, err)
	v, err = s.Get([]byte("c"))
	require.NoError(t, err)
	assert.Equal(t, []byte("d"), v)

	err = s.RunInTransaction(func(txn Txn) error {
		err := txn.Set([]byte("e"), []byte("f"))
		require.NoError(t, err)

		err = txn.Delete([]byte("c"))
		require.NoError(t, err)

		return fmt.Errorf("unexpected error")
	})
	assert.Equal(t, fmt.Errorf("unexpected error"), err)

	_, err = s.Get([]byte("e"))
	assert.Equal(t, KeyNotFoundError, err)
	v, err = s.Get([]byte("c"))
	require.NoError(t, err)
	assert.Equal(t, []byte("d"), v)
}
