// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package ref

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kvcommon "github.com/wrgl/core/pkg/kv/common"
	kvtestutils "github.com/wrgl/core/pkg/kv/testutils"
	"github.com/wrgl/core/pkg/objects"
	reftestutils "github.com/wrgl/core/pkg/ref/testutils"
)

func TestParseNavigationChars(t *testing.T) {
	for _, c := range []struct {
		commitStr, name string
		goBack          int
	}{
		{"my-branch^", "my-branch", 1},
		{"my-branch", "my-branch", 0},
		{"my-branch^^", "my-branch", 2},
		{"my-branch~0", "my-branch", 0},
		{"my-branch~4", "my-branch", 4},
	} {
		name, goBack, err := parseNavigationChars(c.commitStr)
		require.NoError(t, err)
		assert.Equal(t, c.name, name)
		assert.Equal(t, c.goBack, goBack)
	}
}

func TestGetPrevCommit(t *testing.T) {
	db := kvtestutils.NewMockStore(false)
	sum1, commit1 := reftestutils.SaveTestCommit(t, db, nil)
	sum2, commit2 := reftestutils.SaveTestCommit(t, db, [][]byte{sum1})
	sum3, commit3 := reftestutils.SaveTestCommit(t, db, [][]byte{sum2})

	sum, commit, err := peelCommit(db, sum3, commit3, 0)
	require.NoError(t, err)
	assert.Equal(t, sum3, sum)
	objects.AssertCommitEqual(t, commit3, commit)

	sum, commit, err = peelCommit(db, sum3, commit3, 1)
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	objects.AssertCommitEqual(t, commit2, commit)

	sum, commit, err = peelCommit(db, sum3, commit3, 2)
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	objects.AssertCommitEqual(t, commit1, commit)
}

func TestInterpretCommitName(t *testing.T) {
	db := kvtestutils.NewMockStore(false)
	fs := kvtestutils.NewMockStore(false)
	sum1, commit1 := reftestutils.SaveTestCommit(t, db, nil)
	sum2, commit2 := reftestutils.SaveTestCommit(t, db, [][]byte{sum1})
	branchName := "my-branch"
	err := CommitHead(db, fs, branchName, sum2, commit2)
	require.NoError(t, err)

	for i, c := range []struct {
		db        kvcommon.DB
		commitStr string
		name      string
		sum       []byte
		commit    *objects.Commit
		err       error
	}{
		{db, "my-branch", "refs/heads/my-branch", sum2, commit2, nil},
		{db, "my-branch^", "refs/heads/my-branch", sum1, commit1, nil},
		{db, hex.EncodeToString(sum2), hex.EncodeToString(sum2), sum2, commit2, nil},
		{db, fmt.Sprintf("%s~1", hex.EncodeToString(sum2)), hex.EncodeToString(sum1), sum1, commit1, nil},
		{db, "aaaabbbbccccdddd0000111122223333", "", nil, nil, fmt.Errorf("can't find commit aaaabbbbccccdddd0000111122223333")},
		{db, "some-branch", "", nil, nil, fmt.Errorf("can't find branch some-branch")},
	} {
		name, sum, commit, err := InterpretCommitName(c.db, c.commitStr, false)
		require.Equal(t, c.err, err, "case %d", i)
		assert.Equal(t, c.name, name)
		assert.Equal(t, c.sum, sum)
		if c.commit == nil {
			assert.Nil(t, commit)
		} else {
			objects.AssertCommitEqual(t, c.commit, commit)
		}
	}
}

func TestInterpretRefName(t *testing.T) {
	db := kvtestutils.NewMockStore(false)
	fs := kvtestutils.NewMockStore(false)
	sum1, c1 := reftestutils.SaveTestCommit(t, db, nil)
	require.NoError(t, CommitHead(db, fs, "abc", sum1, c1))
	sum2, c2 := reftestutils.SaveTestCommit(t, db, nil)
	require.NoError(t, SaveTag(db, "abc", sum2))
	sum3, c3 := reftestutils.SaveTestCommit(t, db, nil)
	require.NoError(t, SaveRemoteRef(db, fs, "origin", "abc", sum3, "test", "test@domain.com", "test", "test interpret ref"))
	sum4, c4 := reftestutils.SaveTestCommit(t, db, nil)
	require.NoError(t, SaveTag(db, "def", sum4))
	sum5, c5 := reftestutils.SaveTestCommit(t, db, nil)
	require.NoError(t, SaveRemoteRef(db, fs, "origin", "def", sum5, "test", "test@domain.com", "test", "test interpret ref"))
	sum6, c6 := reftestutils.SaveTestCommit(t, db, nil)
	require.NoError(t, SaveRemoteRef(db, fs, "origin", "ghj", sum6, "test", "test@domain.com", "test", "test interpret ref"))
	sum7, c7 := reftestutils.SaveTestCommit(t, db, nil)
	require.NoError(t, SaveRef(db, fs, "custom/ghj", sum7, "test", "test@domain.com", "test", "test interpret ref"))

	name, sum, c, err := InterpretCommitName(db, "abc", false)
	require.NoError(t, err)
	assert.Equal(t, "refs/heads/abc", name)
	assert.Equal(t, sum1, sum)
	objects.AssertCommitEqual(t, c1, c)

	name, sum, c, err = InterpretCommitName(db, "tags/abc", false)
	require.NoError(t, err)
	assert.Equal(t, "refs/tags/abc", name)
	assert.Equal(t, sum2, sum)
	objects.AssertCommitEqual(t, c2, c)

	name, sum, c, err = InterpretCommitName(db, "refs/remotes/origin/abc", false)
	require.NoError(t, err)
	assert.Equal(t, "refs/remotes/origin/abc", name)
	assert.Equal(t, sum3, sum)
	objects.AssertCommitEqual(t, c3, c)

	name, sum, c, err = InterpretCommitName(db, "def", false)
	require.NoError(t, err)
	assert.Equal(t, "refs/tags/def", name)
	assert.Equal(t, sum4, sum)
	objects.AssertCommitEqual(t, c4, c)

	name, sum, c, err = InterpretCommitName(db, "def", true)
	require.NoError(t, err)
	assert.Equal(t, "refs/remotes/origin/def", name)
	assert.Equal(t, sum5, sum)
	objects.AssertCommitEqual(t, c5, c)

	name, sum, c, err = InterpretCommitName(db, "ghj", false)
	require.NoError(t, err)
	assert.Equal(t, "refs/remotes/origin/ghj", name)
	assert.Equal(t, sum6, sum)
	objects.AssertCommitEqual(t, c6, c)

	name, sum, c, err = InterpretCommitName(db, "custom/ghj", false)
	require.NoError(t, err)
	assert.Equal(t, "refs/custom/ghj", name)
	assert.Equal(t, sum7, sum)
	objects.AssertCommitEqual(t, c7, c)
}
