package doctor

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/conf"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
	"github.com/wrgl/wrgl/pkg/slice"
)

func getRows(t *testing.T, db objects.Store, tbl *objects.Table) [][]string {
	var rows [][]string
	var bb []byte
	var err error
	var blk [][]string
	for _, sum := range tbl.Blocks {
		blk, bb, err = objects.GetBlock(db, bb, sum)
		require.NoError(t, err)
		rows = append(rows, blk...)
	}
	return rows
}

func indexRows(pk []uint32, rows [][]string) map[string][]string {
	m := map[string][]string{}
	for _, row := range rows {
		m[strings.Join(slice.IndicesToValues(row, pk), "-")] = row
	}
	return m
}

func assertDuplicatedRowsRemoved(t *testing.T, db objects.Store, newTbl, oldTbl *objects.Table) {
	t.Helper()
	assert.Equal(t, newTbl.Columns, oldTbl.Columns)
	assert.Equal(t, newTbl.PK, oldTbl.PK)
	assert.Less(t, newTbl.RowsCount, oldTbl.RowsCount)
	newRows := getRows(t, db, newTbl)
	oldRows := getRows(t, db, oldTbl)
	assert.Less(t, len(newRows), len(oldRows))
	assert.Equal(t, indexRows(newTbl.PK, newRows), indexRows(oldTbl.PK, oldRows))
}

func TestResolve(t *testing.T) {
	db := objmock.NewStore()
	rs, close := refmock.NewStore(t)
	defer close()
	d := NewDoctor(db, rs, conf.User{
		Name:  "test user",
		Email: "test@user.com",
	}, testr.New(t))

	type resolveTestCase struct {
		Branch    string
		Factories []commitFactory
		CheckTree func(t *testing.T, db objects.Store, newHead, oldHead *objects.Commit)
	}

	cases := []resolveTestCase{
		{
			Branch: "remove",
			Factories: []commitFactory{
				commitNormal(),
				commitMissingTable(),
				commitNormal(),
			},
			CheckTree: func(t *testing.T, db objects.Store, newHead, oldHead *objects.Commit) {
				assert.Equal(t, newHead.Table, oldHead.Table)
				assert.Empty(t, newHead.Parents)
			},
		},
		{
			Branch: "reingest",
			Factories: []commitFactory{
				commitNormal(),
				commitWithDuplicatedRows(),
				commitNormal(),
			},
			CheckTree: func(t *testing.T, db objects.Store, newHead, oldHead *objects.Commit) {
				assert.Equal(t, newHead.Table, oldHead.Table)
				newCom := getParent(t, db, newHead)
				oldCom := getParent(t, db, oldHead)
				assertDuplicatedRowsRemoved(t, db, getTable(t, db, newCom.Table), getTable(t, db, oldCom.Table))
				assert.Equal(t, newCom.Parents, oldCom.Parents)
			},
		},
		{
			Branch: "resetpk",
			Factories: []commitFactory{
				commitNormal(),
				commitTableWithPKOutOfRange(),
				commitNormal(),
			},
			CheckTree: func(t *testing.T, db objects.Store, newHead, oldHead *objects.Commit) {
				assert.Equal(t, newHead.Table, oldHead.Table)
				newCom := getParent(t, db, newHead)
				oldCom := getParent(t, db, oldHead)
				newTbl := getTable(t, db, newCom.Table)
				oldTbl := getTable(t, db, oldCom.Table)
				assert.Len(t, newTbl.PK, 0)
				assert.Equal(t, newTbl.Columns, oldTbl.Columns)
				assert.Equal(t, getRows(t, db, newTbl), getRows(t, db, oldTbl))
				assert.Equal(t, newCom.Parents, oldCom.Parents)
			},
		},
		{
			Branch: "combined",
			Factories: []commitFactory{
				commitMissingTable(),
				commitTableWithPKOutOfRange(),
				commitWithDuplicatedRows(),
			},
			CheckTree: func(t *testing.T, db objects.Store, newHead, oldHead *objects.Commit) {
				assertDuplicatedRowsRemoved(t, db, getTable(t, db, newHead.Table), getTable(t, db, oldHead.Table))
				newCom := getParent(t, db, newHead)
				oldCom := getParent(t, db, oldHead)
				newTbl := getTable(t, db, newCom.Table)
				oldTbl := getTable(t, db, oldCom.Table)
				assert.Len(t, newTbl.PK, 0)
				assert.Equal(t, newTbl.Columns, oldTbl.Columns)
				assert.Equal(t, getRows(t, db, newTbl), getRows(t, db, oldTbl))
				assert.Empty(t, newCom.Parents)
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oldHeads := map[string]*objects.Commit{}
	for _, c := range cases {
		com, _, _ := writeCommitTree(t, db, c.Factories...)
		require.NoError(t, ref.CommitHead(rs, c.Branch, com.Sum, com, nil))
		oldHeads[c.Branch] = com
		t.Logf("heads/%s: (old) %x", c.Branch, com.Sum)
	}

	ch, errCh, err := d.Diagnose(ctx, []string{"heads/"}, nil)
	require.NoError(t, err)

	for refIssues := range ch {
		t.Logf("%d issues from %s", len(refIssues.Issues), refIssues.Ref)
		require.NoError(t, d.Resolve(refIssues.Issues))
		sum, err := ref.GetRef(rs, refIssues.Ref)
		require.NoError(t, err)
		com, err := objects.GetCommit(db, sum)
		require.NoError(t, err)
		for _, c := range cases {
			if "heads/"+c.Branch == refIssues.Ref {
				t.Logf("heads/%s: (new) %x", c.Branch, com.Sum)
				c.CheckTree(t, db, com, oldHeads[c.Branch])
				break
			}
		}
	}

	err, ok := <-errCh
	assert.False(t, ok)
	require.NoError(t, err)
}
