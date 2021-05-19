// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package kv

import (
	"os"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadger(t *testing.T) {
	badgerPath := "/tmp/badger"
	db, err := badger.Open(badger.DefaultOptions(badgerPath))
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			panic(err)
		}
		if err := os.RemoveAll(badgerPath); err != nil {
			panic(err)
		}
	}()
	s := NewBadgerStore(db)

	assert.False(t, s.Exist([]byte("ab")))

	// test Set
	err = s.Set([]byte("ab"), []byte("cd"))
	require.NoError(t, err)

	assert.True(t, s.Exist([]byte("ab")))

	// test Get
	v, err := s.Get([]byte("ab"))
	require.NoError(t, err)
	assert.Equal(t, []byte("cd"), v)

	// test BatchGet
	err = s.Set([]byte("e"), []byte("f"))
	require.NoError(t, err)

	vals, err := s.BatchGet([][]byte{[]byte("e"), []byte("ab")})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("f"), []byte("cd")}, vals)

	exist, err := s.BatchExist([][]byte{nil, []byte("e"), []byte(""), []byte("cdf"), []byte("ab")})
	require.NoError(t, err)
	assert.Equal(t, []bool{false, true, false, false, true}, exist)

	exist, err = s.BatchExist([][]byte{})
	require.NoError(t, err)
	assert.Equal(t, []bool{}, exist)

	exist, err = s.BatchExist(nil)
	require.NoError(t, err)
	assert.Equal(t, []bool{}, exist)

	err = s.Delete([]byte("ab"))
	require.NoError(t, err)

	assert.False(t, s.Exist([]byte("ab")))

	_, err = s.Get([]byte("ab"))
	assert.Equal(t, KeyNotFoundError, err)

	_, err = s.BatchGet([][]byte{[]byte("e"), []byte("ab")})
	assert.Error(t, err)

	vals, err = s.BatchGet([][]byte{[]byte("e")})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("f")}, vals)

	// Test batchset
	err = s.BatchSet(map[string][]byte{
		"123abc/1": []byte("23"),
		"123abc/2": []byte("45"),
	})
	require.NoError(t, err)

	v, err = s.Get([]byte("123abc/1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("23"), v)

	v, err = s.Get([]byte("123abc/2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("45"), v)

	m, err := s.Filter([]byte("123abc/"))
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"123abc/1": []byte("23"),
		"123abc/2": []byte("45"),
	}, m)

	sl, err := s.FilterKey([]byte("123abc/"))
	require.NoError(t, err)
	sort.Slice(sl, func(i, j int) bool { return string(sl[i]) < string(sl[j]) })
	assert.Equal(t, [][]byte{[]byte("123abc/1"), []byte("123abc/2")}, sl)

	// Test Clear
	err = s.Clear([]byte("123abc/"))
	require.NoError(t, err)

	assert.False(t, s.Exist([]byte("123abc/1")))

	assert.False(t, s.Exist([]byte("123abc/2")))
}
