package doctor

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/pckhoi/meow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/conf"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
	"github.com/wrgl/wrgl/pkg/testutils"
)

type parentsKey struct{}

func setParents(ctx context.Context, parents [][]byte) context.Context {
	return context.WithValue(ctx, parentsKey{}, parents)
}

func getParents(ctx context.Context) [][]byte {
	if v := ctx.Value(parentsKey{}); v != nil {
		return v.([][]byte)
	}
	return nil
}

type bufKey struct{}

func getBuffer(ctx context.Context) (*bytes.Buffer, context.Context) {
	if v := ctx.Value(bufKey{}); v != nil {
		return v.(*bytes.Buffer), ctx
	}
	buf := bytes.NewBuffer(nil)
	return buf, context.WithValue(ctx, bufKey{}, buf)
}

type commitFactory func(t *testing.T, ctx context.Context, db objects.Store) (context.Context, *objects.Commit, *objects.Table)

func commitWithTable(tblSum []byte, tbl *objects.Table) commitFactory {
	return func(t *testing.T, ctx context.Context, db objects.Store) (context.Context, *objects.Commit, *objects.Table) {
		_, com := factory.CommitRandomWithTable(t, db, tblSum, getParents(ctx))
		return setParents(ctx, [][]byte{com.Sum}), com, tbl
	}
}

func saveTable(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) ([]byte, context.Context) {
	t.Helper()
	buf, ctx := getBuffer(ctx)
	buf.Reset()
	_, err := tbl.WriteTo(buf)
	require.NoError(t, err)
	sum, err := objects.SaveTable(db, buf.Bytes())
	require.NoError(t, err)
	return sum, ctx
}

func commitWithEditedTable(edit func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table)) commitFactory {
	return func(t *testing.T, ctx context.Context, db objects.Store) (context.Context, *objects.Commit, *objects.Table) {
		origSum := factory.BuildTableN(t, db, 4, 5, []uint32{0})
		tbl, err := objects.GetTable(db, origSum)
		require.NoError(t, err)
		edit(t, ctx, db, tbl)
		sum, ctx := saveTable(t, ctx, db, tbl)
		return commitWithTable(sum, tbl)(t, ctx, db)
	}
}

func createRows(t *testing.T, ncols, nrows, ndup int) [][]string {
	t.Helper()
	cols := strings.Split(testutils.LowerAlphaBytes[:ncols], "")
	rows := make([][]string, 0, nrows+1)
	rows = append(rows, cols)
	for i := 0; i < nrows; i++ {
		rows = append(rows, append(
			[]string{strconv.Itoa(i + 1)},
			strings.Split(testutils.BrokenRandomAlphaNumericString(ncols-1), "")...,
		))
	}
	for j := 0; j < ndup; j++ {
		copy(rows[nrows-ndup+j+1], rows[nrows-ndup+j-1+1])
	}
	return rows
}

func ingestTableFromRows(t *testing.T, db objects.Store, buf *bytes.Buffer, rows [][]string) ([]byte, *objects.Table) {
	t.Helper()
	hash := meow.New(0)
	enc := objects.NewStrListEncoder(true)
	buf.Reset()
	blk := rows[1:]
	cols := rows[0]
	pk := []uint32{0}

	_, err := objects.WriteBlockTo(enc, buf, blk)
	require.NoError(t, err)
	var bb []byte
	blkSum, bb, err := objects.SaveBlock(db, bb, buf.Bytes())
	require.NoError(t, err)

	// save block index
	idx, err := objects.IndexBlock(enc, hash, blk, pk)
	require.NoError(t, err)
	buf.Reset()
	_, err = idx.WriteTo(buf)
	require.NoError(t, err)
	blkIdxSum, _, err := objects.SaveBlockIndex(db, bb, buf.Bytes())
	require.NoError(t, err)

	tbl := &objects.Table{
		Columns:      cols,
		PK:           pk,
		RowsCount:    uint32(len(blk)),
		Blocks:       [][]byte{blkSum},
		BlockIndices: [][]byte{blkIdxSum},
	}
	buf.Reset()
	_, err = tbl.WriteTo(buf)
	require.NoError(t, err)
	sum, err := objects.SaveTable(db, buf.Bytes())
	require.NoError(t, err)
	return sum, tbl
}

func commitNormal() commitFactory {
	return func(t *testing.T, ctx context.Context, db objects.Store) (context.Context, *objects.Commit, *objects.Table) {
		_, com := factory.CommitRandomN(t, db, 4, 5, getParents(ctx))
		setParents(ctx, [][]byte{com.Sum})
		tbl := getTable(t, db, com.Table)
		return ctx, com, tbl
	}
}

func commitMissingTable() commitFactory {
	return commitWithTable(testutils.SecureRandomBytes(16), nil)
}

func commitTableWithPKOutOfRange() commitFactory {
	return commitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.PK = []uint32{10}
	})
}

func commitTableWithEmptyPKColumn() commitFactory {
	return commitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.Columns[0] = ""
	})
}

func commitTableWithEmptyBlockSum() commitFactory {
	return commitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.Blocks[0] = nil
	})
}

func commitWithNonExistentBlock() commitFactory {
	return commitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.Blocks[0] = testutils.SecureRandomBytes(16)
	})
}

func commitWithDuplicatedRows() commitFactory {
	return func(t *testing.T, ctx context.Context, db objects.Store) (context.Context, *objects.Commit, *objects.Table) {
		buf, ctx := getBuffer(ctx)
		sum, tbl := ingestTableFromRows(t, db, buf, createRows(t, 5, 5, 2))
		return commitWithTable(sum, tbl)(t, ctx, db)
	}
}

func commitWithWrongRowsCount() commitFactory {
	return commitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.RowsCount = 3
	})
}

func commitWithWrongBlockIndicesCount() commitFactory {
	return commitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.BlockIndices = nil
	})
}

func commitWithEmptyBlockIndex() commitFactory {
	return commitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.BlockIndices[0] = nil
	})
}

func commitWithNonExistentBlockIndex() commitFactory {
	return commitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		tbl.BlockIndices[0] = testutils.SecureRandomBytes(16)
	})
}

func commitWithWrongBlockIndexRowCount() commitFactory {
	return commitWithEditedTable(func(t *testing.T, ctx context.Context, db objects.Store, tbl *objects.Table) {
		blk := createRows(t, 4, 10, 0)
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

func writeCommitTree(
	t *testing.T,
	db objects.Store,
	commitFactories ...commitFactory,
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

func TestDiagnoseTree(t *testing.T) {
	db := objmock.NewStore()
	rs, close := refmock.NewStore(t)
	defer close()

	d := NewDoctor(db, rs, conf.User{
		Name:  "test user",
		Email: "test@user.com",
	})

	headCommit, commits, tables := writeCommitTree(t, db,
		commitMissingTable(),
		commitTableWithPKOutOfRange(),
		commitTableWithEmptyPKColumn(),
		commitTableWithEmptyBlockSum(),
		commitWithNonExistentBlock(),
		commitWithDuplicatedRows(),
		commitWithWrongRowsCount(),
		commitWithWrongBlockIndicesCount(),
		commitWithEmptyBlockIndex(),
		commitWithNonExistentBlockIndex(),
		commitWithWrongBlockIndexRowCount(),
	)
	require.NoError(t, ref.CommitHead(rs, "alpha", headCommit.Sum, headCommit, nil))

	issues, err := d.diagnoseTree("alpha", headCommit.Sum)
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
			Err:             "error fetching table: unexpected EOF",
			Resolution:      RemoveResolution,
		},
		{
			Ref:             "alpha",
			DescendantCount: 3,
			AncestorCount:   7,
			Commit:          commits[7].Sum,
			Table:           commits[7].Table,
			Err:             "error fetching table: unexpected EOF",
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
			Err:             "error fetching table: unexpected EOF",
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

	headCommit, _, _ = writeCommitTree(t, db, commitNormal(), commitNormal())
	require.NoError(t, ref.CommitHead(rs, "beta", headCommit.Sum, headCommit, nil))
	issues, err = d.diagnoseTree("alpha", headCommit.Sum)
	require.NoError(t, err)
	assert.Len(t, issues, 0)
}
