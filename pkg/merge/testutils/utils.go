package merge_testutils

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/index"
	"github.com/wrgl/core/pkg/merge"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/sorter"
)

func CreateCollector(t *testing.T, db objects.Store, baseCom *objects.Commit) *merge.RowCollector {
	t.Helper()
	discardedRows, err := index.NewHashSet(misc.NewBuffer(nil), 0)
	require.NoError(t, err)
	baseT, err := objects.GetTable(db, baseCom.Table)
	require.NoError(t, err)
	collector, err := merge.NewCollector(db, baseT, discardedRows)
	require.NoError(t, err)
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

func CollectSortedRows(t *testing.T, merger *merge.Merger, removedCols map[int]struct{}) []*sorter.Block {
	t.Helper()
	rows := []*sorter.Block{}
	ch, err := merger.SortedBlocks(removedCols)
	require.NoError(t, err)
	for blk := range ch {
		rows = append(rows, blk)
	}
	require.NoError(t, merger.Error())
	return rows
}

func CreateMerger(t *testing.T, db objects.Store, commits ...[]byte) *merge.Merger {
	base, err := ref.SeekCommonAncestor(db, commits...)
	require.NoError(t, err)
	baseCom, err := objects.GetCommit(db, base)
	require.NoError(t, err)
	baseT, err := objects.GetTable(db, baseCom.Table)
	require.NoError(t, err)
	otherTs := make([]*objects.Table, len(commits))
	otherSums := make([][]byte, len(commits))
	for i, sum := range commits {
		com, err := objects.GetCommit(db, sum)
		require.NoError(t, err)
		otherT, err := objects.GetTable(db, com.Table)
		require.NoError(t, err)
		otherTs[i] = otherT
		otherSums[i] = com.Table
	}
	collector := CreateCollector(t, db, baseCom)
	merger, err := merge.NewMerger(db, collector, 0, baseT, otherTs, baseCom.Table, otherSums)
	require.NoError(t, err)
	return merger
}
