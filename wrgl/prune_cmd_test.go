// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"io"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/versioning"
)

func assertCommitsCount(t *testing.T, db objects.Store, num int) {
	t.Helper()
	sl, err := versioning.GetAllCommitHashes(db)
	require.NoError(t, err)
	assert.Len(t, sl, num)
}

func assertTablesCount(t *testing.T, db objects.Store, num int) {
	t.Helper()
	sl, err := objects.GetAllTableKeys(db)
	require.NoError(t, err)
	assert.Len(t, sl, num)
}

func assertBlocksCount(t *testing.T, db objects.Store, num int) {
	t.Helper()
	sl, err := objects.GetAllBlockKeys(db)
	require.NoError(t, err)
	if len(sl) != num {
		t.Errorf("rows length is %d not %d", len(sl), num)
	}
}

func assertSetEqual(t *testing.T, sl1, sl2 [][]byte) {
	sort.Slice(sl1, func(i, j int) bool { return string(sl1[i]) < string(sl1[j]) })
	sort.Slice(sl2, func(i, j int) bool { return string(sl2[i]) < string(sl2[j]) })
	assert.Equal(t, sl1, sl2)
}

func TestFindAllCommitsToRemove(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	rs := rd.OpenRefStore()
	sum1, _ := factory.CommitHead(t, db, rs, "branch-1", nil, nil)
	sum2, _ := factory.CommitHead(t, db, rs, "branch-1", nil, nil)
	sum3, _ := factory.CommitHead(t, db, rs, "branch-1", nil, nil)
	sum4, _ := factory.CommitHead(t, db, rs, "branch-2", nil, nil)
	require.NoError(t, ref.DeleteHead(rs, "branch-2"))
	require.NoError(t, ref.SaveRef(rs, "heads/branch-1", sum2, "test", "test@domain.com", "test", "test pruning"))

	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	commitsToRemove, survivingCommits, err := findCommitsToRemove(cmd, db, rs)
	require.NoError(t, err)
	assertSetEqual(t, [][]byte{sum1, sum2}, survivingCommits)
	assertSetEqual(t, [][]byte{sum3, sum4}, commitsToRemove)
}

func TestPruneCmdSmallCommits(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum1, _ := factory.CommitHead(t, db, rs, "branch-1", []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, []uint32{0})
	factory.CommitHead(t, db, rs, "branch-1", []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"4,x,c",
	}, []uint32{0})
	factory.CommitHead(t, db, rs, "branch-2", []string{
		"a,b,c",
		"4,q,w",
		"5,a,s",
		"6,z,x",
	}, []uint32{0})
	factory.CommitHead(t, db, rs, "branch-2", []string{
		"a,b,c",
		"4,q,w",
		"5,a,s",
		"7,r,t",
	}, []uint32{0})
	sum2, _ := factory.CommitHead(t, db, rs, "branch-3", []string{
		"a,b,c",
		"4,q,w",
		"5,a,s",
		"6,z,x",
	}, []uint32{0})
	assertCommitsCount(t, db, 5)
	assertTablesCount(t, db, 4)
	assertBlocksCount(t, db, 8)
	require.NoError(t, ref.DeleteHead(rs, "branch-2"))
	require.NoError(t, ref.SaveRef(rs, "heads/branch-1", sum1, "test", "test@domain.com", "test", "test pruning"))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{"prune"})
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	assertCommitsCount(t, db, 2)
	assertTablesCount(t, db, 2)
	assertBlocksCount(t, db, 6)
	m, err := ref.ListHeads(rs)
	require.NoError(t, err)
	assert.Len(t, m, 2)
	assert.Equal(t, sum1, m["branch-1"])
	assert.Equal(t, sum2, m["branch-3"])
}
