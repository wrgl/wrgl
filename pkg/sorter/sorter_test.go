// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package sorter

import (
	"bytes"
	"encoding/csv"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func writeCSV(t *testing.T, rows [][]string, delimiter rune) *os.File {
	t.Helper()
	f, err := testutils.TempFile("", "test_sorter_*")
	require.NoError(t, err)
	w := csv.NewWriter(f)
	w.Comma = delimiter
	require.NoError(t, w.WriteAll(rows))
	w.Flush()
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	return f
}

func sortedRows(t *testing.T, s *Sorter, rowsCount uint32, removeCols map[int]struct{}) []*Rows {
	t.Helper()
	errCh := make(chan error, 1)
	rowsCh := s.SortedRows(removeCols, errCh)
	rowBlocks := make([]*Rows, objects.BlocksCount(rowsCount))
	for obj := range rowsCh {
		rowBlocks[obj.Offset] = obj
	}
	close(errCh)
	err, ok := <-errCh
	assert.False(t, ok)
	require.NoError(t, err)
	return rowBlocks
}

func sortedBlocks(t *testing.T, s *Sorter, rowsCount uint32) []*Block {
	t.Helper()
	errCh := make(chan error, 1)
	blockCh := s.SortedBlocks(nil, errCh)
	blocks := make([]*Block, objects.BlocksCount(rowsCount))
	for obj := range blockCh {
		blocks[obj.Offset] = obj
	}
	close(errCh)
	err, ok := <-errCh
	assert.False(t, ok)
	require.NoError(t, err)
	return blocks
}

func TestSorterSortedRows(t *testing.T) {
	rows := testutils.BuildRawCSV(4, 700)
	f := writeCSV(t, rows, ',')
	defer os.Remove(f.Name())

	s, err := NewSorter(WithRunSize(4096))
	require.NoError(t, err)
	err = s.SortFile(f, rows[0][:1])
	require.NoError(t, err)
	rowBlocks := sortedRows(t, s, 700, nil)
	assert.Equal(t, rows[0], s.Columns)
	assert.Equal(t, []uint32{0}, s.PK)
	assert.Len(t, rowBlocks, 3)
	SortRows(rows[1:], s.PK)
	for i, obj := range rowBlocks {
		require.Equal(t, i, obj.Offset)
		if i < len(rowBlocks)-1 {
			require.Len(t, obj.Rows, 255)
		} else {
			require.Len(t, obj.Rows, 190)
		}
		for j, row := range obj.Rows {
			require.Equal(t, rows[i*255+j+1], row, "i:%d j:%d", i, j)
		}
	}
	assert.NotNil(t, s.TableSummary())
	require.NoError(t, s.Close())

	// sorter run entirely in memory
	f, err = os.Open(f.Name())
	require.NoError(t, err)
	s, err = NewSorter()
	require.NoError(t, err)
	require.NoError(t, s.SortFile(f, rows[0][:1]))
	rowBlocks2 := sortedRows(t, s, 700, nil)
	assert.Equal(t, rowBlocks, rowBlocks2)
	require.NoError(t, s.Close())
}

func TestSorterSortedBlocks(t *testing.T) {
	rows := testutils.BuildRawCSV(4, 700)
	f := writeCSV(t, rows, ',')
	defer os.Remove(f.Name())

	s, err := NewSorter(WithRunSize(4096))
	require.NoError(t, err)
	err = s.SortFile(f, rows[0][:1])
	require.NoError(t, err)
	blocks := sortedBlocks(t, s, 700)
	assert.Equal(t, rows[0], s.Columns)
	assert.Equal(t, []uint32{0}, s.PK)
	assert.Len(t, blocks, 3)
	SortRows(rows[1:], s.PK)
	for i, obj := range blocks {
		require.Equal(t, i, obj.Offset)
		_, blk, err := objects.ReadBlockFrom(bytes.NewReader(obj.Block))
		require.NoError(t, err)
		if i < len(blocks)-1 {
			require.Len(t, blk, 255)
		} else {
			require.Len(t, blk, 190)
		}
		for j, row := range blk {
			require.Equal(t, rows[i*255+j+1], row, "i:%d j:%d", i, j)
		}
		for j, row := range blk {
			require.Equal(t, rows[i*255+j+1], row, "i:%d j:%d", i, j)
			if j == 0 {
				require.Equal(t, obj.PK, row[:1])
			} else {
				require.LessOrEqual(t, obj.PK[0], row[0])
			}
		}
	}
	assert.NotNil(t, s.TableSummary())
	require.NoError(t, s.Close())

	// sorter run entirely in memory
	f, err = os.Open(f.Name())
	require.NoError(t, err)
	s, err = NewSorter()
	require.NoError(t, err)
	require.NoError(t, s.SortFile(f, rows[0][:1]))
	blocks2 := sortedBlocks(t, s, 700)
	assert.Equal(t, blocks, blocks2)
	require.NoError(t, s.Close())

	// sorter return entire row as PK if PK is nil
	f, err = os.Open(f.Name())
	require.NoError(t, err)
	s, err = NewSorter()
	require.NoError(t, err)
	require.NoError(t, s.SortFile(f, nil))
	blocks3 := sortedBlocks(t, s, 700)
	for _, blk := range blocks3 {
		assert.Len(t, blk.PK, 4)
	}
}

func TestSorterAddRow(t *testing.T) {
	s, err := NewSorter()
	require.NoError(t, err)
	s.PK = []uint32{1}
	require.NoError(t, s.AddRow([]string{"1", "a", "q"}))
	require.NoError(t, s.AddRow([]string{"2", "c", "w"}))
	require.NoError(t, s.AddRow([]string{"3", "b", "e"}))
	rowBlocks := sortedRows(t, s, 3, map[int]struct{}{2: {}})
	assert.Equal(t, []*Rows{
		{
			Rows: [][]string{
				{"1", "a"},
				{"3", "b"},
				{"2", "c"},
			},
		},
	}, rowBlocks)
	require.NoError(t, s.Close())
}

func TestSorterDelimiter(t *testing.T) {
	rows := testutils.BuildRawCSV(4, 100)
	f := writeCSV(t, rows, '|')
	defer os.Remove(f.Name())
	s, err := NewSorter(WithDelimiter('|'))
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.SortFile(f, rows[0][:1]))
	rowBlocks := sortedRows(t, s, 100, nil)
	assert.Equal(t, rows[0], s.Columns)
	assert.Equal(t, []uint32{0}, s.PK)
	assert.Len(t, rowBlocks, 1)
	SortRows(rows[1:], s.PK)
	require.Equal(t, 0, rowBlocks[0].Offset)
	require.Len(t, rowBlocks[0].Rows, 100)
	for j, row := range rowBlocks[0].Rows {
		require.Equal(t, rows[j+1], row, "j:%d", j)
	}
	assert.NotNil(t, s.TableSummary())
}
