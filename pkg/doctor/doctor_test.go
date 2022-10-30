package doctor

import (
	"context"
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/conf"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
)

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
		Factories []factory.CommitFactory
		CheckTree func(t *testing.T, db objects.Store, newHead, oldHead *objects.Commit)
	}

	cases := []resolveTestCase{
		{
			Branch: "remove",
			Factories: []factory.CommitFactory{
				factory.CommitNormal(),
				factory.CommitMissingTable(),
				factory.CommitNormal(),
			},
			CheckTree: func(t *testing.T, db objects.Store, newHead, oldHead *objects.Commit) {
				assert.Equal(t, newHead.Table, oldHead.Table)
				assert.Empty(t, newHead.Parents)
			},
		},
		{
			Branch: "reingest",
			Factories: []factory.CommitFactory{
				factory.CommitNormal(),
				factory.CommitWithDuplicatedRows(),
				factory.CommitNormal(),
			},
			CheckTree: func(t *testing.T, db objects.Store, newHead, oldHead *objects.Commit) {
				assert.Equal(t, newHead.Table, oldHead.Table)
				newCom := factory.GetParent(t, db, newHead)
				oldCom := factory.GetParent(t, db, oldHead)
				factory.AssertDuplicatedRowsRemoved(t, db, factory.GetTable(t, db, newCom.Table), factory.GetTable(t, db, oldCom.Table))
				assert.Equal(t, newCom.Parents, oldCom.Parents)
			},
		},
		{
			Branch: "resetpk",
			Factories: []factory.CommitFactory{
				factory.CommitNormal(),
				factory.CommitTableWithPKOutOfRange(),
				factory.CommitNormal(),
			},
			CheckTree: func(t *testing.T, db objects.Store, newHead, oldHead *objects.Commit) {
				assert.Equal(t, newHead.Table, oldHead.Table)
				newCom := factory.GetParent(t, db, newHead)
				oldCom := factory.GetParent(t, db, oldHead)
				newTbl := factory.GetTable(t, db, newCom.Table)
				oldTbl := factory.GetTable(t, db, oldCom.Table)
				assert.Len(t, newTbl.PK, 0)
				assert.Equal(t, newTbl.Columns, oldTbl.Columns)
				assert.Equal(t, factory.GetRows(t, db, newTbl), factory.GetRows(t, db, oldTbl))
				assert.Equal(t, newCom.Parents, oldCom.Parents)
			},
		},
		{
			Branch: "combined",
			Factories: []factory.CommitFactory{
				factory.CommitMissingTable(),
				factory.CommitTableWithPKOutOfRange(),
				factory.CommitWithDuplicatedRows(),
			},
			CheckTree: func(t *testing.T, db objects.Store, newHead, oldHead *objects.Commit) {
				factory.AssertDuplicatedRowsRemoved(t, db, factory.GetTable(t, db, newHead.Table), factory.GetTable(t, db, oldHead.Table))
				newCom := factory.GetParent(t, db, newHead)
				oldCom := factory.GetParent(t, db, oldHead)
				newTbl := factory.GetTable(t, db, newCom.Table)
				oldTbl := factory.GetTable(t, db, oldCom.Table)
				assert.Len(t, newTbl.PK, 0)
				assert.Equal(t, newTbl.Columns, oldTbl.Columns)
				assert.Equal(t, factory.GetRows(t, db, newTbl), factory.GetRows(t, db, oldTbl))
				assert.Empty(t, newCom.Parents)
			},
		},
		{
			Branch: "cached",
			Factories: []factory.CommitFactory{
				factory.CommitWithDuplicatedRows(),
				factory.CommitWithParentTable(),
			},
			CheckTree: func(t *testing.T, db objects.Store, newHead, oldHead *objects.Commit) {
				factory.AssertDuplicatedRowsRemoved(t, db, factory.GetTable(t, db, newHead.Table), factory.GetTable(t, db, oldHead.Table))
				newCom := factory.GetParent(t, db, newHead)
				assert.Equal(t, newHead.Table, newCom.Table)
			},
		},
		{
			Branch: "skipped",
			Factories: []factory.CommitFactory{
				factory.CommitMissingTable(),
				factory.CommitTableWithPKOutOfRange(),
				factory.CommitWithDuplicatedRows(),
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oldHeads := map[string]*objects.Commit{}
	for _, c := range cases {
		com, _, _ := factory.WriteCommitTree(t, db, c.Factories...)
		require.NoError(t, ref.CommitHead(rs, c.Branch, com.Sum, com, nil))
		oldHeads[c.Branch] = com
		t.Logf("heads/%s: (old) %x", c.Branch, com.Sum)
	}

	ch, errCh, err := d.Diagnose(ctx, []string{"heads/"}, nil, []string{"heads/skipped"})
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

	// check skipped branch isn't processed
	sum, err := ref.GetRef(rs, "heads/skipped")
	require.NoError(t, err)
	assert.Equal(t, oldHeads["skipped"].Sum, sum)

	err, ok := <-errCh
	assert.False(t, ok)
	require.NoError(t, err)
}
