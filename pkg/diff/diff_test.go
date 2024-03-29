// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package diff

import (
	"bytes"
	"encoding/csv"
	"io"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/sorter"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestStrSliceEqual(t *testing.T) {
	assert.True(t, strSliceEqual([]string{}, []string{}))
	assert.True(t, strSliceEqual(nil, nil))
	assert.True(t, strSliceEqual([]string{"a"}, []string{"a"}))
	assert.False(t, strSliceEqual([]string{"a"}, []string{"b"}))
	assert.False(t, strSliceEqual([]string{"a"}, nil))
	assert.False(t, strSliceEqual(nil, []string{"b"}))
	assert.False(t, strSliceEqual([]string{"c"}, []string{"c", "d"}))
}

func getTable(t testing.TB, db objects.Store, sum []byte) (*objects.Table, [][]string) {
	t.Helper()
	tbl, err := objects.GetTable(db, sum)
	require.NoError(t, err)
	tblIdx, err := objects.GetTableIndex(db, sum)
	require.NoError(t, err)
	return tbl, tblIdx
}

func TestDiffTables(t *testing.T) {
	db := objmock.NewStore()
	logger := testr.New(t)
	cases := []struct {
		Sum1, Sum2 []byte
		Events     []*objects.Diff
	}{
		{
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,q",
				"2,a",
				"3,z",
			}, []uint32{0}),
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,q",
				"2,a",
				"3,z",
			}, []uint32{0}),
			[]*objects.Diff{},
		},
		{
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,q",
				"2,a",
				"3,z",
			}, nil),
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,q",
				"2,a",
				"3,z",
			}, nil),
			[]*objects.Diff{},
		},
		{
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,q",
				"2,a",
				"3,z",
			}, []uint32{0}),
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,q",
				"2,s",
				"4,x",
			}, []uint32{0}),
			[]*objects.Diff{
				{
					PK:        hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"),
					Sum:       hexToBytes(t, "d5a84d255207bd4bce4a29ca5c82458f"),
					OldSum:    hexToBytes(t, "ff1f6a4585b59abe0c74aa78510be549"),
					Offset:    1,
					OldOffset: 1,
				},
				{
					PK:     hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
					Sum:    hexToBytes(t, "776beabc377528a964029835c5387e86"),
					Offset: 2,
				},
				{
					PK:        hexToBytes(t, "c5e86ba7d7653eec345ae9b6d77ab0cc"),
					OldSum:    hexToBytes(t, "62c10aeb1a926976d3a1775bc22908c0"),
					OldOffset: 2,
				},
			},
		},
		{
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,q",
				"2,a",
				"3,z",
			}, nil),
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,q",
				"2,s",
				"4,x",
			}, nil),
			[]*objects.Diff{
				{
					PK:        hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"),
					Sum:       hexToBytes(t, "d5a84d255207bd4bce4a29ca5c82458f"),
					OldSum:    hexToBytes(t, "ff1f6a4585b59abe0c74aa78510be549"),
					Offset:    1,
					OldOffset: 1,
				},
				{
					PK:     hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
					Sum:    hexToBytes(t, "776beabc377528a964029835c5387e86"),
					Offset: 2,
				},
				{
					PK:        hexToBytes(t, "c5e86ba7d7653eec345ae9b6d77ab0cc"),
					OldSum:    hexToBytes(t, "62c10aeb1a926976d3a1775bc22908c0"),
					OldOffset: 2,
				},
			},
		},
		{
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,q",
				"2,a",
				"3,z",
			}, []uint32{0}),
			factory.BuildTable(t, db, []string{
				"b,a",
				"q,1",
				"s,2",
				"x,4",
			}, []uint32{1}),
			[]*objects.Diff{
				{
					PK:     hexToBytes(t, "fd1c9513cc47feaf59fa9b76008f2521"),
					Sum:    hexToBytes(t, "259e90d5aea433ef8a93efd180cd7676"),
					OldSum: hexToBytes(t, "e0bd96c0a6f16b8c2bd2556f926c60d4"),
				},
				{
					PK:        hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"),
					Sum:       hexToBytes(t, "d5a84d255207bd4bce4a29ca5c82458f"),
					OldSum:    hexToBytes(t, "20cf02720343c9aa3bbf2492e0ce3d45"),
					Offset:    1,
					OldOffset: 1,
				},
				{
					PK:     hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
					Sum:    hexToBytes(t, "776beabc377528a964029835c5387e86"),
					Offset: 2,
				},
				{
					PK:        hexToBytes(t, "c5e86ba7d7653eec345ae9b6d77ab0cc"),
					OldSum:    hexToBytes(t, "89016437c4c697ede0a6a34dba1993b4"),
					OldOffset: 2,
				},
			},
		},
		{
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,q",
				"2,a",
				"3,z",
			}, []uint32{0}),
			factory.BuildTable(t, db, []string{
				"a,c",
				"1,q",
				"2,a",
				"3,z",
			}, []uint32{0}),
			[]*objects.Diff{
				{
					PK:     hexToBytes(t, "fd1c9513cc47feaf59fa9b76008f2521"),
					Sum:    hexToBytes(t, "259e90d5aea433ef8a93efd180cd7676"),
					OldSum: hexToBytes(t, "259e90d5aea433ef8a93efd180cd7676"),
				},
				{
					PK:        hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"),
					Sum:       hexToBytes(t, "d5a84d255207bd4bce4a29ca5c82458f"),
					OldSum:    hexToBytes(t, "d5a84d255207bd4bce4a29ca5c82458f"),
					Offset:    1,
					OldOffset: 1,
				},
				{
					PK:        hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
					Sum:       hexToBytes(t, "776beabc377528a964029835c5387e86"),
					OldSum:    hexToBytes(t, "776beabc377528a964029835c5387e86"),
					Offset:    2,
					OldOffset: 2,
				},
			},
		},
	}
	for i, c := range cases {
		errChan := make(chan error, 10)
		tbl1, tblIdx1 := getTable(t, db, c.Sum1)
		tbl2, tblIdx2 := getTable(t, db, c.Sum2)
		diffChan, _ := DiffTables(db, db, tbl1, tbl2, tblIdx1, tblIdx2, errChan, logger)
		events := []*objects.Diff{}
		for e := range diffChan {
			events = append(events, e)
		}
		close(errChan)
		err, ok := <-errChan
		assert.False(t, ok, "error was %v", err)
		assert.Equal(t, c.Events, events, "case %d", i)
	}
}

func TestDiffEmitUnchangedRow(t *testing.T) {
	db := objmock.NewStore()
	logger := testr.New(t)
	sum1 := factory.BuildTable(t, db, []string{
		"a,b",
		"1,q",
		"2,a",
		"3,z",
	}, []uint32{0})
	sum2 := factory.BuildTable(t, db, []string{
		"a,b",
		"1,q",
		"2,a",
		"3,z",
	}, []uint32{0})
	errChan := make(chan error, 10)
	tbl1, tblIdx1 := getTable(t, db, sum1)
	tbl2, tblIdx2 := getTable(t, db, sum2)
	diffChan, _ := DiffTables(db, db, tbl1, tbl2, tblIdx1, tblIdx2, errChan, logger, WithEmitUnchangedRow())
	close(errChan)
	_, ok := <-errChan
	assert.False(t, ok)
	events := []*objects.Diff{}
	for e := range diffChan {
		events = append(events, e)
	}
	assert.Equal(t, []*objects.Diff{
		{
			PK:     hexToBytes(t, "fd1c9513cc47feaf59fa9b76008f2521"),
			Sum:    hexToBytes(t, "259e90d5aea433ef8a93efd180cd7676"),
			OldSum: hexToBytes(t, "259e90d5aea433ef8a93efd180cd7676"),
		},
		{
			PK:        hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"),
			Sum:       hexToBytes(t, "d5a84d255207bd4bce4a29ca5c82458f"),
			OldSum:    hexToBytes(t, "d5a84d255207bd4bce4a29ca5c82458f"),
			Offset:    1,
			OldOffset: 1,
		},
		{
			PK:        hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
			Sum:       hexToBytes(t, "776beabc377528a964029835c5387e86"),
			OldSum:    hexToBytes(t, "776beabc377528a964029835c5387e86"),
			Offset:    2,
			OldOffset: 2,
		},
	}, events)
}

func TestDiffSameBlockDifferentPK(t *testing.T) {
	db := objmock.NewStore()
	logger := testr.New(t)
	sum1 := factory.BuildTable(t, db, []string{
		"a,b",
		"1,q",
		"2,a",
		"3,z",
	}, []uint32{0})
	// this table has the same block but different pk
	// it should not interfere with diff results below
	factory.BuildTable(t, db, []string{
		"a,b",
		"1,q",
		"2,a",
		"3,z",
	}, []uint32{})
	sum2 := factory.BuildTable(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, []uint32{0})
	errChan := make(chan error, 10)
	tbl1, tblIdx1 := getTable(t, db, sum1)
	tbl2, tblIdx2 := getTable(t, db, sum2)
	diffChan, _ := DiffTables(db, db, tbl1, tbl2, tblIdx1, tblIdx2, errChan, logger)
	close(errChan)
	_, ok := <-errChan
	assert.False(t, ok)
	events := []*objects.Diff{}
	for e := range diffChan {
		assert.NotEmpty(t, e.Sum)
		assert.NotEmpty(t, e.OldSum)
		events = append(events, e)
	}
	assert.Len(t, events, 3)
}

func ingestRawCSV(b *testing.B, db objects.Store, rows [][]string) (*objects.Table, [][]string) {
	b.Helper()
	buf := bytes.NewBuffer(nil)
	require.NoError(b, csv.NewWriter(buf).WriteAll(rows))
	s, err := sorter.NewSorter()
	require.NoError(b, err)
	sum, err := ingest.IngestTable(db, s, io.NopCloser(bytes.NewReader(buf.Bytes())), nil, logr.Discard())
	require.NoError(b, err)
	return getTable(b, db, sum)
}

func BenchmarkDiffRows(b *testing.B) {
	rawCSV1 := testutils.BuildRawCSV(12, b.N)
	rawCSV2 := testutils.ModifiedCSV(rawCSV1, 1)
	db := objmock.NewStore()
	tbl1, tblIdx1 := ingestRawCSV(b, db, rawCSV1)
	tbl2, tblIdx2 := ingestRawCSV(b, db, rawCSV2)
	errChan := make(chan error, 1000)
	b.ResetTimer()
	diffChan, _ := DiffTables(db, db, tbl1, tbl2, tblIdx1, tblIdx2, errChan, logr.Discard())
	for d := range diffChan {
		assert.NotNil(b, d)
	}
	close(errChan)
	_, ok := <-errChan
	assert.False(b, ok)
}
