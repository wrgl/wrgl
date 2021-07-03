package widgets

import (
	"fmt"
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	mergehelpers "github.com/wrgl/core/pkg/merge/helpers"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
)

func tableCell(txt string, style tcell.Style, bg tcell.Color, transparencyDisabled bool) *TableCell {
	cell := NewTableCell(txt).SetStyle(style).DisableTransparency(transparencyDisabled)
	if bg != 0 {
		cell.SetBackgroundColor(bg)
	}
	return cell
}

func assertMergeRowDisplay(t *testing.T, db objects.Store, sums [][]byte, cells [][]*TableCell) {
	t.Helper()
	m, buf := mergehelpers.CreateMerger(t, db, sums...)
	merges := mergehelpers.CollectUnresolvedMerges(t, m)
	names := make([]string, len(sums)+1)
	for i := 0; i < len(sums); i++ {
		names[i] = fmt.Sprintf("branch-%d", i+1)
	}
	names[len(names)-1] = "resolution"
	mr := NewMergeRow(buf, merges[0].ColDiff, names)
	require.NoError(t, mr.DisplayMerge(merges[1]))
	assert.Equal(t, cells, mr.Cells)
}

func TestMergeRow(t *testing.T) {
	db := objmock.NewStore()
	base, _ := factory.Commit(t, db, []string{
		"a,b,c,e",
		"1,q,w,r",
	}, []uint32{0}, nil)
	sum1, _ := factory.Commit(t, db, []string{
		"a,b,c,e",
		"1,a,w,s",
	}, []uint32{0}, [][]byte{base})
	sum2, _ := factory.Commit(t, db, []string{
		"a,b,d,e",
		"1,d,r,r",
	}, []uint32{0}, [][]byte{base})
	assertMergeRowDisplay(t, db, [][]byte{sum1, sum2}, [][]*TableCell{
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
	})

	base, _ = factory.Commit(t, db, []string{
		"a,b,c",
		"2,a,c",
	}, []uint32{0}, nil)
	sum1, _ = factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
	}, []uint32{0}, [][]byte{base})
	sum2, _ = factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,s",
	}, []uint32{0}, [][]byte{base})
	assertMergeRowDisplay(t, db, [][]byte{sum1, sum2}, [][]*TableCell{
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
	})

	base, _ = factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
	}, []uint32{0}, nil)
	sum1, _ = factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
	}, []uint32{0}, [][]byte{base})
	sum2, _ = factory.Commit(t, db, []string{
		"a,b,c",
		"2,a,s",
	}, []uint32{0}, [][]byte{base})
	sum3, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,s",
	}, []uint32{0}, [][]byte{base})
	assertMergeRowDisplay(t, db, [][]byte{sum1, sum2, sum3}, [][]*TableCell{
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
			tableCell("q", cellStyle, 0, false),
			tableCell("s", yellowStyle, 0, false),
		},
	})
}

func TestMergeRowPool(t *testing.T) {
	db := objmock.NewStore()
	base, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, []uint32{0}, nil)
	sum1, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,a",
		"2,d,s",
		"3,z,v",
	}, []uint32{0}, [][]byte{base})
	sum2, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,s",
		"2,f,s",
		"3,z,c",
	}, []uint32{0}, [][]byte{base})
	m, buf := mergehelpers.CreateMerger(t, db, sum1, sum2)
	merges := mergehelpers.CollectUnresolvedMerges(t, m)
	mp := NewMergeRowPool(buf, merges[0].ColDiff, []string{"branch-1", "branch-2", "resolution"}, merges[1:])
	assert.Equal(t, tableCell("q", cellStyle, 0, false), mp.GetCell(1, 2, 1))
	assert.Equal(t, tableCell("x", cellStyle, tcell.ColorDarkRed, true), mp.GetCell(0, 3, 2))
	mp.SetCell(0, 2, "v", false)
	assert.Equal(t, tableCell("v", yellowStyle, 0, false), mp.GetCell(0, 3, 2))
	assert.True(t, mp.IsTextAtCellDifferentFromBase(2, 2, 0))
	assert.False(t, mp.IsTextAtCellDifferentFromBase(2, 3, 0))
}
