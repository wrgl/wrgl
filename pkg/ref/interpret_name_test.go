// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ref_test

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	objhelpers "github.com/wrgl/wrgl/pkg/objects/helpers"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
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
		name, goBack, err := ref.ParseNavigationChars(c.commitStr)
		require.NoError(t, err)
		assert.Equal(t, c.name, name)
		assert.Equal(t, c.goBack, goBack)
	}
}

func TestGetPrevCommit(t *testing.T) {
	db := objmock.NewStore()
	sum1, commit1 := refhelpers.SaveTestCommit(t, db, nil)
	sum2, commit2 := refhelpers.SaveTestCommit(t, db, [][]byte{sum1})
	sum3, commit3 := refhelpers.SaveTestCommit(t, db, [][]byte{sum2})

	sum, commit, err := ref.PeelCommit(db, sum3, commit3, 0)
	require.NoError(t, err)
	assert.Equal(t, sum3, sum)
	objhelpers.AssertCommitEqual(t, commit3, commit)

	sum, commit, err = ref.PeelCommit(db, sum3, commit3, 1)
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	objhelpers.AssertCommitEqual(t, commit2, commit)

	sum, commit, err = ref.PeelCommit(db, sum3, commit3, 2)
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	objhelpers.AssertCommitEqual(t, commit1, commit)
}

func TestInterpretCommitName(t *testing.T) {
	db := objmock.NewStore()
	rs := refmock.NewStore()
	sum1, commit1 := refhelpers.SaveTestCommit(t, db, nil)
	sum2, commit2 := refhelpers.SaveTestCommit(t, db, [][]byte{sum1})
	branchName := "my-branch"
	require.NoError(t, ref.CommitHead(rs, branchName, sum2, commit2))

	for i, c := range []struct {
		commitStr string
		name      string
		sum       []byte
		commit    *objects.Commit
		err       error
	}{
		{"my-branch", "heads/my-branch", sum2, commit2, nil},
		{"my-branch^", "heads/my-branch", sum1, commit1, nil},
		{hex.EncodeToString(sum2), hex.EncodeToString(sum2), sum2, commit2, nil},
		{fmt.Sprintf("%s~1", hex.EncodeToString(sum2)), hex.EncodeToString(sum1), sum1, commit1, nil},
		{"aaaabbbbccccdddd0000111122223333", "", nil, nil, fmt.Errorf("can't find commit aaaabbbbccccdddd0000111122223333")},
		{"some-branch", "", nil, nil, fmt.Errorf("can't find branch some-branch")},
	} {
		name, sum, commit, err := ref.InterpretCommitName(db, rs, c.commitStr, false)
		require.Equal(t, c.err, err, "case %d", i)
		assert.Equal(t, c.name, name)
		assert.Equal(t, c.sum, sum)
		if c.commit == nil {
			assert.Nil(t, commit)
		} else {
			objhelpers.AssertCommitEqual(t, c.commit, commit)
		}
	}
}

func TestInterpretRefName(t *testing.T) {
	db := objmock.NewStore()
	rs := refmock.NewStore()
	sum1, c1 := refhelpers.SaveTestCommit(t, db, nil)
	require.NoError(t, ref.CommitHead(rs, "abc", sum1, c1))
	sum2, c2 := refhelpers.SaveTestCommit(t, db, nil)
	require.NoError(t, ref.SaveTag(rs, "abc", sum2))
	sum3, c3 := refhelpers.SaveTestCommit(t, db, nil)
	require.NoError(t, ref.SaveRemoteRef(rs, "origin", "abc", sum3, "test", "test@domain.com", "test", "test interpret ref"))
	sum4, c4 := refhelpers.SaveTestCommit(t, db, nil)
	require.NoError(t, ref.SaveTag(rs, "def", sum4))
	sum5, c5 := refhelpers.SaveTestCommit(t, db, nil)
	require.NoError(t, ref.SaveRemoteRef(rs, "origin", "def", sum5, "test", "test@domain.com", "test", "test interpret ref"))
	sum6, c6 := refhelpers.SaveTestCommit(t, db, nil)
	require.NoError(t, ref.SaveRemoteRef(rs, "origin", "ghj", sum6, "test", "test@domain.com", "test", "test interpret ref"))
	sum7, c7 := refhelpers.SaveTestCommit(t, db, nil)
	require.NoError(t, ref.SaveRef(rs, "custom/ghj", sum7, "test", "test@domain.com", "test", "test interpret ref"))

	name, sum, c, err := ref.InterpretCommitName(db, rs, "abc", false)
	require.NoError(t, err)
	assert.Equal(t, "heads/abc", name)
	assert.Equal(t, sum1, sum)
	objhelpers.AssertCommitEqual(t, c1, c)

	name, sum, c, err = ref.InterpretCommitName(db, rs, "tags/abc", false)
	require.NoError(t, err)
	assert.Equal(t, "tags/abc", name)
	assert.Equal(t, sum2, sum)
	objhelpers.AssertCommitEqual(t, c2, c)

	name, sum, c, err = ref.InterpretCommitName(db, rs, "remotes/origin/abc", false)
	require.NoError(t, err)
	assert.Equal(t, "remotes/origin/abc", name)
	assert.Equal(t, sum3, sum)
	objhelpers.AssertCommitEqual(t, c3, c)

	name, sum, c, err = ref.InterpretCommitName(db, rs, "def", false)
	require.NoError(t, err)
	assert.Equal(t, "tags/def", name)
	assert.Equal(t, sum4, sum)
	objhelpers.AssertCommitEqual(t, c4, c)

	name, sum, c, err = ref.InterpretCommitName(db, rs, "def", true)
	require.NoError(t, err)
	assert.Equal(t, "remotes/origin/def", name)
	assert.Equal(t, sum5, sum)
	objhelpers.AssertCommitEqual(t, c5, c)

	name, sum, c, err = ref.InterpretCommitName(db, rs, "ghj", false)
	require.NoError(t, err)
	assert.Equal(t, "remotes/origin/ghj", name)
	assert.Equal(t, sum6, sum)
	objhelpers.AssertCommitEqual(t, c6, c)

	name, sum, c, err = ref.InterpretCommitName(db, rs, "custom/ghj", false)
	require.NoError(t, err)
	assert.Equal(t, "custom/ghj", name)
	assert.Equal(t, sum7, sum)
	objhelpers.AssertCommitEqual(t, c7, c)
}
