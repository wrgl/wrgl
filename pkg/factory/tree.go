package factory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/testutils"
)

type CommitFactory func(t *testing.T, ctx context.Context, db objects.Store) (context.Context, *objects.Commit, *objects.Table)

func CommitWithTable(tblSum []byte, tbl *objects.Table) CommitFactory {
	return func(t *testing.T, ctx context.Context, db objects.Store) (context.Context, *objects.Commit, *objects.Table) {
		_, com := CommitRandomWithTable(t, db, tblSum, getParents(ctx))
		return setParents(ctx, [][]byte{com.Sum}), com, tbl
	}
}

func CommitWithEditedTable(edit func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table)) CommitFactory {
	return func(t *testing.T, ctx context.Context, db objects.Store) (context.Context, *objects.Commit, *objects.Table) {
		origSum := BuildTableN(t, db, 4, 5, []uint32{0})
		tbl, err := objects.GetTable(db, origSum)
		require.NoError(t, err)
		edit(t, ctx, db, tbl)
		sum, ctx := SaveTable(t, ctx, db, tbl)
		return CommitWithTable(sum, tbl)(t, ctx, db)
	}
}

func CommitNormal() CommitFactory {
	return func(t *testing.T, ctx context.Context, db objects.Store) (context.Context, *objects.Commit, *objects.Table) {
		_, com := CommitRandomN(t, db, 4, 5, getParents(ctx))
		setParents(ctx, [][]byte{com.Sum})
		tbl := GetTable(t, db, com.Table)
		return ctx, com, tbl
	}
}

func CommitMissingTable() CommitFactory {
	return CommitWithTable(testutils.SecureRandomBytes(16), nil)
}

func CommitTableWithPKOutOfRange() CommitFactory {
	return CommitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.PK = []uint32{10}
	})
}

func CommitWithParentTable() CommitFactory {
	return func(t *testing.T, ctx context.Context, db objects.Store) (context.Context, *objects.Commit, *objects.Table) {
		parents := getParents(ctx)
		parentCom := GetCommit(t, db, parents[0])
		return CommitWithTable(parentCom.Table, GetTable(t, db, parentCom.Table))(t, ctx, db)
	}
}

func CommitTableWithEmptyPKColumn() CommitFactory {
	return CommitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.Columns[0] = ""
	})
}

func CommitTableWithEmptyBlockSum() CommitFactory {
	return CommitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.Blocks[0] = nil
	})
}

func CommitWithNonExistentBlock() CommitFactory {
	return CommitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.Blocks[0] = testutils.SecureRandomBytes(16)
	})
}

func CommitWithDuplicatedRows() CommitFactory {
	return func(t *testing.T, ctx context.Context, db objects.Store) (context.Context, *objects.Commit, *objects.Table) {
		buf, ctx := getBuffer(ctx)
		sum, tbl := ingestTableFromRows(t, db, buf, CreateRows(t, 5, 5, 2))
		return CommitWithTable(sum, tbl)(t, ctx, db)
	}
}

func CommitWithWrongRowsCount() CommitFactory {
	return CommitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.RowsCount = 3
	})
}

func CommitWithWrongBlockIndicesCount() CommitFactory {
	return CommitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.BlockIndices = nil
	})
}

func CommitWithEmptyBlockIndex() CommitFactory {
	return CommitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.BlockIndices[0] = nil
	})
}

func CommitWithNonExistentBlockIndex() CommitFactory {
	return CommitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.BlockIndices[0] = testutils.SecureRandomBytes(16)
	})
}

func CommitWithWrongBlockIndexRowCount() CommitFactory {
	return CommitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		blk := CreateRows(t, 4, 10, 0)
		buf, _ := getBuffer(ctx)
		enc := objects.NewStrListEncoder(false)
		buf.Reset()
		_, err := objects.WriteBlockTo(enc, buf, blk[1:])
		require.NoError(t, err)
		blkSum, _, err := objects.SaveBlock(db, nil, buf.Bytes())
		require.NoError(t, err)
		tbl.Blocks[0] = blkSum
		tbl.RowsCount = 10
	})
}

func WriteCommitTree(
	t *testing.T,
	db objects.Store,
	commitFactories ...CommitFactory,
) (headCommit *objects.Commit, commits []*objects.Commit, tables []*objects.Table) {
	t.Helper()
	var com *objects.Commit
	var tbl *objects.Table
	commits = []*objects.Commit{}
	tables = []*objects.Table{}
	ctx := context.Background()
	for _, fac := range commitFactories {
		ctx, com, tbl = fac(t, ctx, db)
		commits = append(commits, com)
		tables = append(tables, tbl)
	}
	return com, commits, tables
}
