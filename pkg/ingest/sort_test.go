package ingest

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

func createRandomCSV(columns []string, n int) [][]string {
	rows := make([][]string, n+1)
	rows[0] = columns
	m := len(columns)
	for i := 0; i < n; i++ {
		rows[i+1] = make([]string, m)
		for j := 0; j < m; j++ {
			rows[i+1][j] = testutils.BrokenRandomAlphaNumericString(5)
		}
	}
	return rows
}

func writeCSV(t *testing.T, rows [][]string) string {
	t.Helper()
	f, err := ioutil.TempFile("", "test_sorter_*")
	require.NoError(t, err)
	w := csv.NewWriter(f)
	require.NoError(t, w.WriteAll(rows))
	w.Flush()
	require.NoError(t, f.Close())
	return f.Name()
}

func collectBlocks(t *testing.T, s *Sorter, rowsCount uint32) []*block {
	t.Helper()
	errCh := make(chan error, 1)
	blkCh := s.EmitChunks(errCh)
	blocks := make([]*block, objects.BlocksCount(700))
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
	rows := createRandomCSV([]string{"a", "b", "c", "d"}, 700)
	name := writeCSV(t, rows)
	defer os.Remove(name)

	s, err := NewSorter(name, []string{"a"}, 4096, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "d"}, s.Columns)
	assert.Equal(t, []uint32{0}, s.PK)
	blocks := collectBlocks(t, s, 700)
	assert.Len(t, blocks, 3)
	sortBlock(rows[1:], s.PK)
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
	s, err = NewSorter(name, []string{"a"}, 0, io.Discard)
	require.NoError(t, err)
	blocks2 := collectBlocks(t, s, 700)
	assert.Equal(t, blocks, blocks2)
}
