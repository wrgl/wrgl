// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func assertCommitsCount(t *testing.T, db objects.Store, num int) {
	t.Helper()
	sl, err := objects.GetAllCommitKeys(db)
	require.NoError(t, err)
	assert.Len(t, sl, num)
}

func assertTablesCount(t *testing.T, db objects.Store, num int) {
	t.Helper()
	sl, err := objects.GetAllTableKeys(db)
	require.NoError(t, err)
	assert.Len(t, sl, num)
}

func assertTableIndicesCount(t *testing.T, db objects.Store, num int) {
	t.Helper()
	sl, err := objects.GetAllTableIndexKeys(db)
	require.NoError(t, err)
	assert.Len(t, sl, num)
}

func assertTableProfilesCount(t *testing.T, db objects.Store, num int) {
	t.Helper()
	sl, err := objects.GetAllTableProfileKeys(db)
	require.NoError(t, err)
	assert.Len(t, sl, num)
}

func assertBlocksCount(t *testing.T, db objects.Store, num int) {
	t.Helper()
	sl, err := objects.GetAllBlockKeys(db)
	require.NoError(t, err)
	if len(sl) != num {
		t.Errorf("blocks count is %d not %d", len(sl), num)
	}
}

func assertBlockIndicesCount(t *testing.T, db objects.Store, num int) {
	t.Helper()
	sl, err := objects.GetAllBlockIndexKeys(db)
	require.NoError(t, err)
	if len(sl) != num {
		sums := make([]string, len(sl))
		for i, b := range sl {
			sums[i] = fmt.Sprintf("  %x", b)
		}
		t.Errorf("block indices count is %d not %d:\n%s", len(sl), num, strings.Join(sums, "\n"))
	}
}

func assertSetEqual(t *testing.T, sl1, sl2 [][]byte) {
	sort.Slice(sl1, func(i, j int) bool { return string(sl1[i]) < string(sl1[j]) })
	sort.Slice(sl2, func(i, j int) bool { return string(sl2[i]) < string(sl2[j]) })
	assert.Equal(t, sl1, sl2)
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
	assertTableIndicesCount(t, db, 4)
	assertTableProfilesCount(t, db, 4)
	assertBlocksCount(t, db, 4)
	assertBlockIndicesCount(t, db, 4)
	require.NoError(t, ref.DeleteHead(rs, "branch-2"))
	require.NoError(t, ref.SaveRef(rs, "heads/branch-1", sum1, "test", "test@domain.com", "test", "test pruning", nil))
	require.NoError(t, db.Close())

	cmd := rootCmd()
	cmd.SetArgs([]string{"prune"})
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	assertCommitsCount(t, db, 2)
	assertTablesCount(t, db, 2)
	assertTableIndicesCount(t, db, 2)
	assertTableProfilesCount(t, db, 2)
	assertBlocksCount(t, db, 2)
	assertBlockIndicesCount(t, db, 2)
	m, err := ref.ListHeads(rs)
	require.NoError(t, err)
	assert.Len(t, m, 2)
	assert.Equal(t, sum1, m["branch-1"])
	assert.Equal(t, sum2, m["branch-3"])
}
