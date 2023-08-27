package doctor

import (
	"context"
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/conf"
	"github.com/wrgl/wrgl/pkg/factory"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
)

func TestDiagnoseTree(t *testing.T) {
	db := objmock.NewStore()
	rs, close := refmock.NewStore(t)
	defer close()

	d := NewDoctor(db, rs, conf.User{
		Name:  "test user",
		Email: "test@user.com",
	}, testr.New(t))

	headCommit, commits, tables := factory.WriteCommitTree(t, db,
		factory.CommitMissingTable(),
		factory.CommitTableWithPKOutOfRange(),
		factory.CommitTableWithEmptyPKColumn(),
		factory.CommitTableWithEmptyBlockSum(),
		factory.CommitWithNonExistentBlock(),
		factory.CommitWithDuplicatedRows(),
		factory.CommitWithWrongRowsCount(),
		factory.CommitWithWrongBlockIndicesCount(),
		factory.CommitWithEmptyBlockIndex(),
		factory.CommitWithNonExistentBlockIndex(),
		factory.CommitWithWrongBlockIndexRowCount(),
	)
	require.NoError(t, ref.CommitHead(rs, "alpha", headCommit.Sum, headCommit, nil))

	tableIssues := map[string]Issue{}

	issues, err := d.diagnoseTree(tableIssues, "alpha", headCommit.Sum)
	require.NoError(t, err)
	assert.Equal(t, []*Issue{
		{
			Ref:             "alpha",
			DescendantCount: 0,
			AncestorCount:   10,
			Commit:          commits[10].Sum,
			Table:           commits[10].Table,
			Err:             "index rows count does not match: 5 vs 10",
			Resolution:      ReingestResolution,
		},
		{
			Ref:             "alpha",
			DescendantCount: 1,
			AncestorCount:   9,
			Commit:          commits[9].Sum,
			Table:           commits[9].Table,
			BlockIndex:      tables[9].BlockIndices[0],
			Err:             "error getting block index: key not found",
			Resolution:      ReingestResolution,
		},
		{
			Ref:             "alpha",
			DescendantCount: 2,
			AncestorCount:   8,
			Commit:          commits[8].Sum,
			Table:           commits[8].Table,
			Err:             "error fetching table: unexpected EOF reading block index (0/1)",
			Resolution:      RemoveResolution,
		},
		{
			Ref:             "alpha",
			DescendantCount: 3,
			AncestorCount:   7,
			Commit:          commits[7].Sum,
			Table:           commits[7].Table,
			Err:             "error fetching table: unexpected EOF reading block index (0/1)",
			Resolution:      RemoveResolution,
		},
		{
			Ref:             "alpha",
			DescendantCount: 4,
			AncestorCount:   6,
			Commit:          commits[6].Sum,
			Table:           commits[6].Table,
			Err:             "rows count does not match: 5 vs 3",
			Resolution:      ReingestResolution,
		},
		{
			Ref:             "alpha",
			DescendantCount: 5,
			AncestorCount:   5,
			Commit:          commits[5].Sum,
			Table:           commits[5].Table,
			Block:           tables[5].Blocks[0],
			Err:             "duplicated rows: 2,3",
			Resolution:      ReingestResolution,
		},
		{
			Ref:             "alpha",
			DescendantCount: 6,
			AncestorCount:   4,
			Commit:          commits[4].Sum,
			Table:           commits[4].Table,
			Block:           tables[4].Blocks[0],
			Err:             "error getting block: key not found",
			Resolution:      RemoveResolution,
		},
		{
			Ref:             "alpha",
			DescendantCount: 7,
			AncestorCount:   3,
			Commit:          commits[3].Sum,
			Table:           commits[3].Table,
			Err:             "error fetching table: unexpected EOF reading block index (0/1)",
			Resolution:      RemoveResolution,
		},
		{
			Ref:             "alpha",
			DescendantCount: 8,
			AncestorCount:   2,
			Commit:          commits[2].Sum,
			Table:           commits[2].Table,
			Err:             "primary key column is empty: 0",
			Resolution:      ResetPKResolution,
		},
		{
			Ref:             "alpha",
			DescendantCount: 9,
			AncestorCount:   1,
			Commit:          commits[1].Sum,
			Table:           commits[1].Table,
			Err:             "pk index greater than columns count: [0]: 10",
			Resolution:      ResetPKResolution,
		},
		{
			Ref:             "alpha",
			DescendantCount: 10,
			AncestorCount:   0,
			Commit:          commits[0].Sum,
			Table:           commits[0].Table,
			Err:             "error fetching table: key not found",
			Resolution:      RemoveResolution,
		},
	}, issues)

	headCommit, _, _ = factory.WriteCommitTree(t, db, factory.CommitNormal(), factory.CommitNormal())
	require.NoError(t, ref.CommitHead(rs, "beta", headCommit.Sum, headCommit, nil))
	issues, err = d.diagnoseTree(tableIssues, "alpha", headCommit.Sum)
	require.NoError(t, err)
	assert.Len(t, issues, 0)

	// Test table cache does not affect returned issues
	_, com1, _ := factory.CommitWithDuplicatedRows()(t, context.Background(), db)
	_, com2 := factory.CommitRandomWithTable(t, db, com1.Table, [][]byte{com1.Sum})
	require.NoError(t, ref.CommitHead(rs, "cached", com2.Sum, com2, nil))
	issues, err = d.diagnoseTree(tableIssues, "cached", com2.Sum)
	require.NoError(t, err)
	require.Len(t, issues, 2)
	assert.Equal(t, issues[0].Table, issues[1].Table)
	assert.Equal(t, issues[0].Err, issues[1].Err)
	assert.Equal(t, issues[0].Resolution, issues[1].Resolution)
	assert.NotEqual(t, issues[0].Commit, issues[1].Commit)
}
