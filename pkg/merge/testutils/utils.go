package merge_testutils

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/index"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/merge"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
)

func CreateCollector(t *testing.T, db kv.DB, fs kv.FileStore, baseCom *objects.Commit) *merge.RowCollector {
	t.Helper()
	resolvedRows, err := merge.NewSortableRows(misc.NewBuffer(nil), misc.NewBuffer(nil), []int{1})
	require.NoError(t, err)
	discardedRows, err := index.NewHashSet(misc.NewBuffer(nil), 0)
	require.NoError(t, err)
	baseT, err := table.ReadTable(db, fs, baseCom.Table)
	require.NoError(t, err)
	collector := merge.NewCollector(db, baseT, resolvedRows, discardedRows)
	return collector
}

func CollectUnresolvedMerges(t *testing.T, merger *merge.Merger) []*merge.Merge {
	t.Helper()
	mergeCh, err := merger.Start()
	require.NoError(t, err)
	merges := []*merge.Merge{}
	for m := range mergeCh {
		merges = append(merges, m)
	}
	sort.SliceStable(merges, func(i, j int) bool {
		if merges[i].ColDiff != nil && merges[j].ColDiff == nil {
			return true
		}
		if merges[j].ColDiff != nil && merges[i].ColDiff == nil {
			return false
		}
		if merges[i].Base == nil && merges[j].Base != nil {
			return true
		}
		if merges[j].Base == nil && merges[i].Base != nil {
			return false
		}
		return string(merges[i].Base) < string(merges[j].Base)
	})
	return merges
}

func CollectSortedRows(t *testing.T, merger *merge.Merger, removedCols map[int]struct{}) [][]string {
	t.Helper()
	rows := [][]string{}
	ch, err := merger.SortedRows(removedCols)
	require.NoError(t, err)
	for sl := range ch {
		rows = append(rows, sl)
	}
	require.NoError(t, merger.Error())
	return rows
}
