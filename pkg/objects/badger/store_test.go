// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objbadger

import (
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
)

func TestStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
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
	defer s.Close()

	assert.False(t, s.Exist([]byte("ab")))

	// test Set
	err = s.Set([]byte("ab"), []byte("cd"))
	require.NoError(t, err)

	assert.True(t, s.Exist([]byte("ab")))

	// test Get
	v, err := s.Get([]byte("ab"))
	require.NoError(t, err)
	assert.Equal(t, []byte("cd"), v)

	err = s.Set([]byte("e"), []byte("f"))
	require.NoError(t, err)

	err = s.Delete([]byte("ab"))
	require.NoError(t, err)

	assert.False(t, s.Exist([]byte("ab")))

	_, err = s.Get([]byte("ab"))
	assert.Equal(t, objects.ErrKeyNotFound, err)

	require.NoError(t, s.Set([]byte("123abc/1"), []byte("23")))
	require.NoError(t, s.Set([]byte("123abc/2"), []byte("45")))
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
