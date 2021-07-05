package sorter

import (
	"encoding/csv"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
)

func writeCSV(t *testing.T, rows [][]string) *os.File {
	t.Helper()
	f, err := ioutil.TempFile("", "test_sorter_*")
	require.NoError(t, err)
	w := csv.NewWriter(f)
	require.NoError(t, w.WriteAll(rows))
	w.Flush()
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	return f
}

func collectBlocks(t *testing.T, s *Sorter, rowsCount uint32, remCols map[int]struct{}) []*Block {
	t.Helper()
	errCh := make(chan error, 1)
	blkCh := s.SortedBlocks(remCols, errCh)
	blocks := make([]*Block, objects.BlocksCount(rowsCount))
	for blk := range blkCh {
		blocks[blk.Offset] = blk
	}
	close(errCh)
	err, ok := <-errCh
	assert.False(t, ok)
	require.NoError(t, err)
	return blocks
}

func sortedBlocks(t *testing.T, s *Sorter, f io.ReadCloser, pk []string, rowsCount uint32) []*Block {
	t.Helper()
	errCh := make(chan error, 1)
	blkCh, err := s.SortFile(f, pk, errCh)
	require.NoError(t, err)
	blocks := make([]*Block, objects.BlocksCount(rowsCount))
	for blk := range blkCh {
		blocks[blk.Offset] = blk
	}
	close(errCh)
	err, ok := <-errCh
	assert.False(t, ok)
	require.NoError(t, err)
	return blocks
}

func TestSorterSortFile(t *testing.T) {
	rows := testutils.BuildRawCSV(4, 700)
	f := writeCSV(t, rows)
	defer os.Remove(f.Name())

	s, err := NewSorter(4096, io.Discard)
	require.NoError(t, err)
	blocks := sortedBlocks(t, s, f, rows[0][:1], 700)
	assert.Equal(t, rows[0], s.Columns)
	assert.Equal(t, []uint32{0}, s.PK)
	assert.Len(t, blocks, 3)
	SortBlock(rows[1:], s.PK)
	for i, blk := range blocks {
		require.Equal(t, i, blk.Offset)
		if i < len(blocks)-1 {
			require.Len(t, blk.Block, 255)
		} else {
			require.Len(t, blk.Block, 190)
		}
		for j, row := range blk.Block {
			require.Equal(t, rows[i*255+j+1], row, "i:%d j:%d", i, j)
			if j == 0 {
				require.Equal(t, blk.PK, row[:1])
			} else {
				require.LessOrEqual(t, blk.PK[0], row[0])
			}
		}
	}
	require.NoError(t, s.Close())

	// sorter run entirely in memory
	f, err = os.Open(f.Name())
	require.NoError(t, err)
	s, err = NewSorter(0, io.Discard)
	require.NoError(t, err)
	blocks2 := sortedBlocks(t, s, f, rows[0][:1], 700)
	assert.Equal(t, blocks, blocks2)
	require.NoError(t, s.Close())
}

func TestSorterAddRow(t *testing.T) {
	s, err := NewSorter(0, io.Discard)
	require.NoError(t, err)
	s.PK = []uint32{1}
	require.NoError(t, s.AddRow([]string{"1", "a", "q"}))
	require.NoError(t, s.AddRow([]string{"2", "c", "w"}))
	require.NoError(t, s.AddRow([]string{"3", "b", "e"}))
	blocks := collectBlocks(t, s, 3, map[int]struct{}{2: {}})
	assert.Equal(t, []*Block{
		{
			Block: [][]string{
				{"1", "a"},
				{"3", "b"},
				{"2", "c"},
			},
			PK: []string{"a"},
		},
	}, blocks)
	require.NoError(t, s.Close())
}
