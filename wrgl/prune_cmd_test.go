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

func assertTablesCount(t *testing.T, db kv.Store, fs kv.FileStore, num int) {
	t.Helper()
	sl, err := table.GetAllTableHashes(db, fs)
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

func assertSetEqual(t *testing.T, sl1, sl2 [][]byte) {
	sort.Slice(sl1, func(i, j int) bool { return string(sl1[i]) < string(sl1[j]) })
	sort.Slice(sl2, func(i, j int) bool { return string(sl2[i]) < string(sl2[j]) })
	assert.Equal(t, sl1, sl2)
}

func TestFindAllCommitsToRemove(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	fs := rd.OpenFileStore()
	sum1, _ := factory.Commit(t, db, fs, "branch-1", nil, nil, nil)
	sum2, _ := factory.Commit(t, db, fs, "branch-1", nil, nil, nil)
	sum3, _ := factory.Commit(t, db, fs, "branch-1", nil, nil, nil)
	sum4, _ := factory.Commit(t, db, fs, "branch-2", nil, nil, nil)
	require.NoError(t, versioning.DeleteHead(db, "branch-2"))
	require.NoError(t, versioning.SaveHead(db, "branch-1", sum2))

	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	commitsToRemove, survivingCommits, err := findCommitsToRemove(cmd, db)
	require.NoError(t, err)
	assertSetEqual(t, [][]byte{sum1, sum2}, survivingCommits)
	assertSetEqual(t, [][]byte{sum3, sum4}, commitsToRemove)
}

func TestPruneCmdSmallCommits(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cf, cleanup := createConfigFile(t)
	defer cleanup()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	sum1, _ := factory.Commit(t, db, fs, "branch-1", []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, []uint32{0}, nil)
	factory.Commit(t, db, fs, "branch-1", []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"4,x,c",
	}, []uint32{0}, nil)
	factory.Commit(t, db, fs, "branch-2", []string{
		"a,b,c",
		"4,q,w",
		"5,a,s",
		"6,z,x",
	}, []uint32{0}, nil)
	factory.Commit(t, db, fs, "branch-2", []string{
		"a,b,c",
		"4,q,w",
		"5,a,s",
		"7,r,t",
	}, []uint32{0}, nil)
	sum2, _ := factory.Commit(t, db, fs, "branch-3", []string{
		"a,b,c",
		"4,q,w",
		"5,a,s",
		"6,z,x",
	}, []uint32{0}, nil)
	assertCommitsCount(t, db, 5)
	assertTablesCount(t, db, fs, 4)
	assertRowsCount(t, db, 8)
	require.NoError(t, versioning.DeleteHead(db, "branch-2"))
	require.NoError(t, versioning.SaveHead(db, "branch-1", sum1))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	setCmdArgs(cmd, rd, cf, "prune")
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	assertCommitsCount(t, db, 2)
	assertTablesCount(t, db, fs, 2)
	assertRowsCount(t, db, 6)
	m, err := versioning.ListHeads(db)
	require.NoError(t, err)
	assert.Len(t, m, 2)
	assert.Equal(t, sum1, m["branch-1"])
	assert.Equal(t, sum2, m["branch-3"])
}
