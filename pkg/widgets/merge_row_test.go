package widgets

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/gdamore/tcell/v2"
	"github.com/mmcloughlin/meow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/merge"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
)

func insertRow(t *testing.T, db kv.DB, row []string) []byte {
	b := objects.NewStrListEncoder().Encode(row)
	sum := meow.Checksum(0, b)
	require.NoError(t, table.SaveRow(db, sum[:], b))
	return sum[:]
}

func tableCell(txt string, style tcell.Style, bg tcell.Color, transparencyDisabled bool) *TableCell {
	cell := NewTableCell(txt).SetStyle(style).DisableTransparency(transparencyDisabled)
	if bg != 0 {
		cell.SetBackgroundColor(bg)
	}
	return cell
}

func TestMergeRow(t *testing.T) {
	db := kv.NewMockStore(false)
	cd := objects.CompareColumns(
		[2][]string{{"a", "b", "c", "e"}, {"a"}},
		[2][]string{{"a", "b", "c", "e"}, {"a"}},
		[2][]string{{"a", "b", "d", "e"}, {"a"}},
	)
	mr := NewMergeRow(
		db, objects.NewStrListDecoder(false), cd, []string{"branch-1", "branch-2", "resolution"},
	)
	rr := merge.NewRowResolver(db, cd)

	m := &merge.Merge{
		Base: insertRow(t, db, []string{"1", "q", "w", "r"}),
		Others: [][]byte{
			insertRow(t, db, []string{"1", "a", "w", "s"}),
			insertRow(t, db, []string{"1", "d", "r", "r"}),
		},
	}
	require.NoError(t, rr.Resolve(m))
	require.NoError(t, mr.DisplayMerge(m))
	assert.Equal(t, [][]*TableCell{
		{
			tableCell("branch-1", boldYellowStyle, 0, false),
			tableCell("1", primaryKeyStyle, 0, false),
			tableCell("a", yellowStyle, 0, false),
			tableCell("", cellStyle, 0, false),
			tableCell("w", cellStyle, 0, false),
			tableCell("s", yellowStyle, 0, false),
		},
		{
			tableCell("branch-2", boldYellowStyle, 0, false),
			tableCell("1", primaryKeyStyle, 0, false),
			tableCell("d", yellowStyle, 0, false),
			tableCell("r", greenStyle, 0, false),
			tableCell("", redStyle, 0, false),
			tableCell("r", cellStyle, 0, false),
		},
		{
			tableCell("resolution", boldStyle, 0, false),
			tableCell("1", primaryKeyStyle, 0, false),
			tableCell("q", cellStyle, tcell.ColorDarkRed, true),
			tableCell("r", greenStyle, 0, false),
			tableCell("", yellowStyle, 0, false),
			tableCell("s", yellowStyle, 0, false),
		},
	}, mr.Cells)
}

func TestMergeRowBothAdded(t *testing.T) {
	db := kv.NewMockStore(false)
	cd := objects.CompareColumns(
		[2][]string{{"a", "b", "c"}, {"a"}},
		[2][]string{{"a", "b", "c"}, {"a"}},
		[2][]string{{"a", "b", "c"}, {"a"}},
	)
	mr := NewMergeRow(
		db, objects.NewStrListDecoder(false), cd, []string{"branch-1", "branch-2", "resolution"},
	)
	rr := merge.NewRowResolver(db, cd)
	m := &merge.Merge{
		Others: [][]byte{
			insertRow(t, db, []string{"1", "q", "w"}),
			insertRow(t, db, []string{"1", "q", "s"}),
		},
	}
	require.NoError(t, rr.Resolve(m))
	require.NoError(t, mr.DisplayMerge(m))
	assert.Equal(t, [][]*TableCell{
		{
			tableCell("branch-1", boldGreenStyle, 0, false),
			tableCell("1", primaryKeyStyle, 0, false),
			tableCell("q", greenStyle, 0, false),
			tableCell("w", greenStyle, 0, false),
		},
		{
			tableCell("branch-2", boldGreenStyle, 0, false),
			tableCell("1", primaryKeyStyle, 0, false),
			tableCell("q", greenStyle, 0, false),
			tableCell("s", greenStyle, 0, false),
		},
		{
			tableCell("resolution", boldRedStyle, 0, false),
			tableCell("1", primaryKeyStyle, 0, false),
			tableCell("q", greenStyle, 0, false),
			tableCell("", greenStyle, tcell.ColorDarkRed, true),
		},
	}, mr.Cells)
}

func TestMergeRowModifiedRemovedUnchanged(t *testing.T) {
	db := kv.NewMockStore(false)
	cd := objects.CompareColumns(
		[2][]string{{"a", "b", "c"}, {"a"}},
		[2][]string{{"a", "b", "c"}, {"a"}},
		[2][]string{{"a", "b", "c"}, {"a"}},
		[2][]string{{"a", "b", "c"}, {"a"}},
	)
	mr := NewMergeRow(
		db, objects.NewStrListDecoder(false), cd, []string{"branch-1", "branch-2", "branch-3", "resolution"},
	)
	rr := merge.NewRowResolver(db, cd)

	m := &merge.Merge{
		Base: insertRow(t, db, []string{"1", "q", "w"}),
		Others: [][]byte{
			insertRow(t, db, []string{"1", "q", "w"}),
			nil,
			insertRow(t, db, []string{"1", "q", "s"}),
		},
	}
	require.NoError(t, rr.Resolve(m))
	t.Log(spew.Sdump(m))
	require.NoError(t, mr.DisplayMerge(m))
	assert.Equal(t, [][]*TableCell{
		{
			tableCell("branch-1", boldStyle, 0, false),
			tableCell("1", primaryKeyStyle, 0, false),
			tableCell("q", cellStyle, 0, false),
			tableCell("w", cellStyle, 0, false),
		},
		{
			tableCell("branch-2", boldRedStyle, 0, false),
			tableCell("", primaryKeyStyle, 0, false),
			tableCell("", cellStyle, 0, false),
			tableCell("", cellStyle, 0, false),
		},
		{
			tableCell("branch-3", boldYellowStyle, 0, false),
			tableCell("1", primaryKeyStyle, 0, false),
			tableCell("q", cellStyle, 0, false),
			tableCell("s", yellowStyle, 0, false),
		},
		{
			tableCell("resolution", boldStyle, 0, false),
			tableCell("1", primaryKeyStyle, 0, false),
			tableCell("q", cellStyle, tcell.ColorDarkRed, true),
			tableCell("w", cellStyle, tcell.ColorDarkRed, true),
		},
	}, mr.Cells)
}

func TestMergeRowPool(t *testing.T) {
	db := kv.NewMockStore(false)
	cd := objects.CompareColumns(
		[2][]string{{"a", "b", "c"}, {"a"}},
		[2][]string{{"a", "b", "c"}, {"a"}},
		[2][]string{{"a", "b", "c"}, {"a"}},
	)
	rr := merge.NewRowResolver(db, cd)
	merges := []*merge.Merge{
		{
			Base: insertRow(t, db, []string{"1", "q", "w"}),
			Others: [][]byte{
				insertRow(t, db, []string{"1", "q", "a"}),
				insertRow(t, db, []string{"1", "q", "s"}),
			},
		},
		{
			Base: insertRow(t, db, []string{"2", "a", "s"}),
			Others: [][]byte{
				insertRow(t, db, []string{"2", "d", "s"}),
				insertRow(t, db, []string{"2", "f", "s"}),
			},
		},
		{
			Base: insertRow(t, db, []string{"3", "z", "x"}),
			Others: [][]byte{
				insertRow(t, db, []string{"3", "z", "v"}),
				insertRow(t, db, []string{"3", "z", "c"}),
			},
		},
	}
	for _, m := range merges {
		require.NoError(t, rr.Resolve(m))
	}
	mp := NewMergeRowPool(db, cd, []string{"branch-1", "branch-2", "resolution"}, merges)
	assert.Equal(t, tableCell("q", cellStyle, 0, false), mp.GetCell(0, 2, 1))
	assert.Equal(t, tableCell("x", cellStyle, tcell.ColorDarkRed, true), mp.GetCell(2, 3, 2))
	mp.SetCell(2, 2, "v", false)
	assert.Equal(t, tableCell("v", yellowStyle, 0, false), mp.GetCell(2, 3, 2))
	assert.True(t, mp.IsTextAtCellDifferentFromBase(1, 2, 0))
	assert.False(t, mp.IsTextAtCellDifferentFromBase(1, 3, 0))
}
