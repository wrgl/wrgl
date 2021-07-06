package widgets

import (
	"sort"
	"testing"

	"github.com/rivo/tview"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/merge"
	mergehelpers "github.com/wrgl/core/pkg/merge/helpers"
	objmock "github.com/wrgl/core/pkg/objects/mock"
)

func TestMergeApp(t *testing.T) {
	db := objmock.NewStore()
	base, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
	}, []uint32{0}, nil)
	com1, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,r",
		"2,f,s",
		"3,v,c",
		"4,r,t",
	}, []uint32{0}, [][]byte{base})
	com2, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,t",
		"3,s,d",
	}, []uint32{0}, [][]byte{base})
	merger, buf := mergehelpers.CreateMerger(t, db, com1, com2)
	app := tview.NewApplication()
	ma := NewMergeApp(buf, merger, app, []string{"branch-1", "branch-2"}, [][]byte{com1, com2}, base)
	mch, err := merger.Start()
	require.NoError(t, err)
	merges := []*merge.Merge{}
	cd := (<-mch).ColDiff
	for m := range mch {
		merges = append(merges, m)
	}
	ma.InitializeTable(cd, merges)
	sort.Slice(ma.merges, func(i, j int) bool {
		return string(ma.merges[i].PK) < string(ma.merges[j].PK)
	})
	assert.Len(t, ma.merges, 3)
	assert.Equal(t, []string{"2", "a", "s"}, ma.merges[0].ResolvedRow)
	assert.Equal(t, map[uint32]struct{}{1: {}}, ma.merges[0].UnresolvedCols)
	assert.Equal(t, []string{"3", "", ""}, ma.merges[1].ResolvedRow)
	assert.Equal(t, map[uint32]struct{}{1: {}, 2: {}}, ma.merges[1].UnresolvedCols)
	assert.Equal(t, []string{"1", "q", "w"}, ma.merges[2].ResolvedRow)
	assert.Equal(t, map[uint32]struct{}{2: {}}, ma.merges[2].UnresolvedCols)
	assert.Len(t, ma.resolvedRows, 0)

	ma.setCellFromLayer(2, 2, 1)
	assert.Equal(t, []string{"1", "q", "t"}, ma.merges[2].ResolvedRow)
	assert.Equal(t, map[uint32]struct{}{}, ma.merges[2].UnresolvedCols)
	assert.Contains(t, ma.resolvedRows, 2)

	ma.undo()
	assert.Equal(t, []string{"1", "q", "w"}, ma.merges[2].ResolvedRow)
	assert.Equal(t, map[uint32]struct{}{2: {}}, ma.merges[2].UnresolvedCols)
	assert.Len(t, ma.resolvedRows, 0)

	ma.redo()
	assert.Equal(t, []string{"1", "q", "t"}, ma.merges[2].ResolvedRow)
	assert.Equal(t, map[uint32]struct{}{}, ma.merges[2].UnresolvedCols)
	assert.Contains(t, ma.resolvedRows, 2)

	ma.deleteRow(0)
	assert.Contains(t, ma.resolvedRows, 0)
	assert.Contains(t, ma.removedRows, 0)

	ma.setCellFromLayer(0, 1, 0)
	assert.Equal(t, []string{"2", "f", "s"}, ma.merges[0].ResolvedRow)
	assert.Contains(t, ma.resolvedRows, 0)
	assert.NotContains(t, ma.removedRows, 0)

	ma.undo()
	assert.Contains(t, ma.resolvedRows, 0)
	assert.Contains(t, ma.removedRows, 0)

	ma.undo()
	assert.NotContains(t, ma.resolvedRows, 0)
	assert.NotContains(t, ma.removedRows, 0)

	ma.setCellFromLayer(1, 1, 1)
	assert.Equal(t, []string{"3", "s", ""}, ma.merges[1].ResolvedRow)
	assert.Equal(t, map[uint32]struct{}{2: {}}, ma.merges[1].UnresolvedCols)
	assert.NotContains(t, ma.resolvedRows, 1)

	ma.deleteColumn(2)
	assert.Equal(t, []string{"3", "s", ""}, ma.merges[1].ResolvedRow)
	assert.Len(t, ma.merges[1].UnresolvedCols, 0)
	assert.Contains(t, ma.resolvedRows, 1)
	assert.Contains(t, ma.RemovedCols, 2)

	ma.undo()
	assert.Contains(t, ma.merges[1].UnresolvedCols, uint32(2))
	assert.NotContains(t, ma.resolvedRows, 1)
	assert.NotContains(t, ma.RemovedCols, 2)

	ma.redo()
	ma.setCellFromLayer(1, 2, 0)
	assert.Equal(t, []string{"3", "s", "c"}, ma.merges[1].ResolvedRow)
	assert.Len(t, ma.merges[1].UnresolvedCols, 0)
	assert.Contains(t, ma.resolvedRows, 1)
	assert.NotContains(t, ma.RemovedCols, 2)

	ma.undo()
	assert.Equal(t, []string{"3", "s", ""}, ma.merges[1].ResolvedRow)
	assert.Len(t, ma.merges[1].UnresolvedCols, 0)
	assert.Equal(t, map[int]struct{}{1: {}, 2: {}}, ma.resolvedRows)
	assert.Contains(t, ma.RemovedCols, 2)

	ma.unresolveRow(2)
	assert.False(t, ma.merges[2].Resolved)
	assert.NotContains(t, ma.resolvedRows, 2)

	ma.resolveRow(2)
	assert.True(t, ma.merges[2].Resolved)
	assert.Contains(t, ma.resolvedRows, 2)

	ma.undo()
	assert.False(t, ma.merges[2].Resolved)
	assert.NotContains(t, ma.resolvedRows, 2)

	ma.undo()
	assert.True(t, ma.merges[2].Resolved)
	assert.Contains(t, ma.resolvedRows, 2)
}
