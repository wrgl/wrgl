// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
)

func TestFindRemoteThatMightHaveCommit(t *testing.T) {
	db := objmock.NewStore()
	rs, cleanup := refmock.NewStore(t)
	defer cleanup()

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

	require.NoError(t, ref.CommitHead(rs, "main", sum1, c1, nil))
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
