// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package diff

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
)

func TestRowChangeReader(t *testing.T) {
	db := objmock.NewStore()
	sum1 := factory.BuildTable(t, db, []string{
		"a,b,c,d",
		"1,2,3,4",
		"2,2,3,4",
		"3,2,7,4",
	}, []uint32{0})
	sum2 := factory.BuildTable(t, db, []string{
		"a,b,c,e",
		"1,2,3,5",
		"2,6,3,5",
		"3,2,3,5",
	}, []uint32{0})
	tbl1, tblIdx1 := getTable(t, db, sum1)
	tbl2, tblIdx2 := getTable(t, db, sum2)
	errCh := make(chan error, 1)
	diffCh, _ := DiffTables(db, db, tbl1, tbl2, tblIdx1, tblIdx2, errCh)
	close(errCh)
	err, ok := <-errCh
	assert.False(t, ok)
	require.NoError(t, err)
	diffs := []*objects.Diff{}
	for d := range diffCh {
		diffs = append(diffs, d)
	}
	assert.Len(t, diffs, 3)

	// test Read
	reader, err := NewRowChangeReader(
		db, db, tbl1, tbl2,
		CompareColumns([2][]string{{"a", "b", "c", "e"}, {"a"}}, [2][]string{{"a", "b", "c", "d"}, {"a"}}),
	)
	require.NoError(t, err)
	reader.AddRowDiff(diffs[0])
	assert.Equal(t, 1, reader.Len())
	mr, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"1"}, {"2"}, {"3"}, {"5"}, {"4"},
	}, mr)

	// test seek
	reader.AddRowDiff(diffs[1])
	reader.AddRowDiff(diffs[2])
	assert.Equal(t, 3, reader.Len())
	reader.Seek(1, io.SeekCurrent)
	mr, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"3"}, {"2"}, {"7", "3"}, {"5"}, {"4"},
	}, mr)
	reader.Seek(-2, io.SeekEnd)
	mr, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"2"}, {"2", "6"}, {"3"}, {"5"}, {"4"},
	}, mr)
	reader.Seek(0, io.SeekStart)
	mr, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"1"}, {"2"}, {"3"}, {"5"}, {"4"},
	}, mr)

	// test readAt
	mr, err = reader.ReadAt(1)
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"2"}, {"2", "6"}, {"3"}, {"5"}, {"4"},
	}, mr)
}
