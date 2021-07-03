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

func collectBlocks(t *testing.T, s *Sorter, rowsCount uint32) []*Block {
	t.Helper()
	errCh := make(chan error, 1)
	blkCh := s.SortedBlocks(errCh)
	blocks := make([]*Block, objects.BlocksCount(700))
	for blk := range blkCh {
		blocks[blk.Offset] = blk
	}
	close(errCh)
	err, ok := <-errCh
	assert.False(t, ok)
	require.NoError(t, err)
	return blocks
}

func TestSorter(t *testing.T) {
	rows := testutils.BuildRawCSV(4, 700)
	f := writeCSV(t, rows)
	defer os.Remove(f.Name())

	s, err := NewSorter(f, f.Name(), rows[0][:1], 4096, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, rows[0], s.Columns)
	assert.Equal(t, []uint32{0}, s.PK)
	blocks := collectBlocks(t, s, 700)
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

	// sorter run entirely in memory
	f, err = os.Open(f.Name())
	require.NoError(t, err)
	s, err = NewSorter(f, f.Name(), rows[0][:1], 0, io.Discard)
	require.NoError(t, err)
	blocks2 := collectBlocks(t, s, 700)
	assert.Equal(t, blocks, blocks2)
}
