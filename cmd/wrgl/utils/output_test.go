// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
)

func TestFindRemoteThatMightHaveCommit(t *testing.T) {
	db := objmock.NewStore()
	rs := refmock.NewStore()

	f, err := NewRemoteFinder(db, rs)
	require.NoError(t, err)
	_, err = f.FindRemoteFor(nil)
	assert.Equal(t, "empty commit sum", err.Error())

	sum1, c1 := factory.CommitRandom(t, db, nil)
	f, err = NewRemoteFinder(db, rs)
	require.NoError(t, err)
	remote, err := f.FindRemoteFor(sum1)
	require.NoError(t, err)
	assert.Empty(t, remote)

	require.NoError(t, ref.CommitHead(rs, "main", sum1, c1))
	f, err = NewRemoteFinder(db, rs)
	require.NoError(t, err)
	remote, err = f.FindRemoteFor(sum1)
	require.NoError(t, err)
	assert.Empty(t, remote)

	sum2, _ := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.SaveFetchRef(rs, "remotes/origin/abc", sum2, "John", "john@doe.com", "origin", "storing ref"))
	f, err = NewRemoteFinder(db, rs)
	require.NoError(t, err)
	remote, err = f.FindRemoteFor(sum2)
	require.NoError(t, err)
	assert.Equal(t, "origin", remote)

	sum3, _ := factory.CommitRandom(t, db, [][]byte{sum1})
	require.NoError(t, ref.SaveFetchRef(rs, "remotes/origin2/def", sum3, "John", "john@doe.com", "origin2", "storing ref"))
	f, err = NewRemoteFinder(db, rs)
	require.NoError(t, err)
	remote, err = f.FindRemoteFor(sum1)
	require.NoError(t, err)
	assert.Equal(t, "origin2", remote)
}

func TestGetTable(t *testing.T) {
	db := objmock.NewStore()
	rs := refmock.NewStore()

	_, c1 := factory.CommitRandom(t, db, nil)
	tbl, err := GetTable(db, rs, c1)
	require.NoError(t, err)
	assert.Equal(t, c1.Table, tbl.Sum)

	sum2, c2 := refhelpers.SaveTestCommit(t, db, nil)
	tbl, err = GetTable(db, rs, c2)
	assert.Equal(t, fmt.Errorf("table %x not found", c2.Table), err)
	assert.Nil(t, tbl)

	sum3, _ := factory.CommitRandom(t, db, [][]byte{sum2})
	require.NoError(t, ref.SaveFetchRef(rs, "remotes/origin/abc", sum3, "john", "john@doe.com", "origin", "storing ref"))
	tbl, err = GetTable(db, rs, c2)
	assert.Equal(t, fmt.Errorf("table %x not found, try fetching it with:\n  wrgl fetch tables origin %x", c2.Table, c2.Table), err)
	assert.Nil(t, tbl)
}
