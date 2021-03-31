package main

import (
	"io"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
)

func assertCommitsCount(t *testing.T, db kv.Store, num int) {
	t.Helper()
	sl, err := versioning.GetAllCommitHashes(db)
	require.NoError(t, err)
	assert.Len(t, sl, num)
}

func assertSmallTablesCount(t *testing.T, db kv.Store, num int) {
	t.Helper()
	sl, err := table.GetAllSmallTableHashes(db)
	require.NoError(t, err)
	assert.Len(t, sl, num)
}

func assertBigTablesCount(t *testing.T, db kv.Store, num int) {
	t.Helper()
	sl, err := table.GetAllBigTableHashes(db)
	require.NoError(t, err)
	assert.Len(t, sl, num)
}

func assertRowsCount(t *testing.T, db kv.Store, num int) {
	t.Helper()
	sl, err := table.GetAllRowKeys(db)
	require.NoError(t, err)
	if len(sl) != num {
		t.Errorf("rows length is %d not %d", len(sl), num)
	}
}

func assertSetEqual(t *testing.T, sl1, sl2 []string) {
	sort.Strings(sl1)
	sort.Strings(sl2)
	assert.Equal(t, sl1, sl2)
}

func TestFindAllCommitsToRemove(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	sum1, _ := factory.CommitSmall(t, db, "branch-1", nil, nil, nil)
	sum2, _ := factory.CommitSmall(t, db, "branch-1", nil, nil, nil)
	sum3, _ := factory.CommitSmall(t, db, "branch-1", nil, nil, nil)
	sum4, _ := factory.CommitSmall(t, db, "branch-2", nil, nil, nil)
	require.NoError(t, versioning.DeleteBranch(db, "branch-2"))
	b := &versioning.Branch{CommitHash: sum2}
	require.NoError(t, b.Save(db, "branch-1"))

	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	commitsToRemove, survivingCommits, err := findCommitsToRemove(cmd, db)
	require.NoError(t, err)
	assertSetEqual(t, []string{sum1, sum2}, survivingCommits)
	assertSetEqual(t, []string{sum3, sum4}, commitsToRemove)
}

func TestPruneCmdSmallCommits(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	sum1, _ := factory.CommitSmall(t, db, "branch-1", []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, []int{0}, nil)
	factory.CommitSmall(t, db, "branch-1", []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"4,x,c",
	}, []int{0}, nil)
	factory.CommitSmall(t, db, "branch-2", []string{
		"a,b,c",
		"4,q,w",
		"5,a,s",
		"6,z,x",
	}, []int{0}, nil)
	factory.CommitSmall(t, db, "branch-2", []string{
		"a,b,c",
		"4,q,w",
		"5,a,s",
		"7,r,t",
	}, []int{0}, nil)
	sum2, _ := factory.CommitSmall(t, db, "branch-3", []string{
		"a,b,c",
		"4,q,w",
		"5,a,s",
		"6,z,x",
	}, []int{0}, nil)
	assertCommitsCount(t, db, 5)
	assertSmallTablesCount(t, db, 4)
	assertRowsCount(t, db, 8)
	require.NoError(t, versioning.DeleteBranch(db, "branch-2"))
	b := &versioning.Branch{CommitHash: sum1}
	require.NoError(t, b.Save(db, "branch-1"))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	setCmdArgs(cmd, rd, "prune")
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	assertCommitsCount(t, db, 2)
	assertSmallTablesCount(t, db, 2)
	assertRowsCount(t, db, 6)
	m, err := versioning.ListBranch(db)
	require.NoError(t, err)
	assert.Equal(t, map[string]*versioning.Branch{
		"branch-1": {CommitHash: sum1},
		"branch-3": {CommitHash: sum2},
	}, m)
}

func TestPruneCmdBigCommits(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	sum1, _ := factory.CommitBig(t, db, fs, "branch-1", []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, []int{0}, nil)
	factory.CommitBig(t, db, fs, "branch-1", []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"4,x,c",
	}, []int{0}, nil)
	factory.CommitBig(t, db, fs, "branch-2", []string{
		"a,b,c",
		"4,q,w",
		"5,a,s",
		"6,z,x",
	}, []int{0}, nil)
	factory.CommitBig(t, db, fs, "branch-2", []string{
		"a,b,c",
		"4,q,w",
		"5,a,s",
		"7,r,t",
	}, []int{0}, nil)
	sum2, _ := factory.CommitBig(t, db, fs, "branch-3", []string{
		"a,b,c",
		"4,q,w",
		"5,a,s",
		"6,z,x",
	}, []int{0}, nil)
	assertCommitsCount(t, db, 5)
	assertBigTablesCount(t, db, 4)
	assertRowsCount(t, db, 8)
	require.NoError(t, versioning.DeleteBranch(db, "branch-2"))
	b := &versioning.Branch{CommitHash: sum1}
	require.NoError(t, b.Save(db, "branch-1"))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	setCmdArgs(cmd, rd, "prune")
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	assertCommitsCount(t, db, 2)
	assertBigTablesCount(t, db, 2)
	assertRowsCount(t, db, 6)
	m, err := versioning.ListBranch(db)
	require.NoError(t, err)
	assert.Equal(t, map[string]*versioning.Branch{
		"branch-1": {CommitHash: sum1},
		"branch-3": {CommitHash: sum2},
	}, m)
}
