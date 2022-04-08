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

func TestGetTable(t *testing.T) {
	db := objmock.NewStore()
	rs, cleanup := refmock.NewStore(t)
	defer cleanup()

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

func TestIsWrglhubRemote(t *testing.T) {
	_, _, ok := IsWrglhubRemote("https://my-repo")
	assert.False(t, ok)
	username, reponame, ok := IsWrglhubRemote("https://hub.wrgl.co/api/users/my-username/repos/my-repo/")
	assert.True(t, ok)
	assert.Equal(t, "my-username", username)
	assert.Equal(t, "my-repo", reponame)
	username, reponame, ok = IsWrglhubRemote("https://hub.wrgl.co/api/users/my-username/repos/my-repo")
	assert.True(t, ok)
	assert.Equal(t, "my-username", username)
	assert.Equal(t, "my-repo", reponame)
}

func TestRepoWebURI(t *testing.T) {
	assert.Equal(t, "https://hub.wrgl.co/@my-username/r/my-repo/", RepoWebURI("my-username", "my-repo"))
}
