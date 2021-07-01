// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"bytes"
	"encoding/csv"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/kv"
	kvcommon "github.com/wrgl/core/pkg/kv/common"
	kvfs "github.com/wrgl/core/pkg/kv/fs"
	kvtestutils "github.com/wrgl/core/pkg/kv/testutils"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
)

func getTable(t testing.TB, db kvcommon.DB, sum []byte) (*objects.Table, [][]string) {
	t.Helper()
	b, err := kv.GetTable(db, sum)
	require.NoError(t, err)
	_, tbl, err := objects.ReadTableFrom(bytes.NewReader(b))
	require.NoError(t, err)
	b, err = kv.GetTableIndex(db, sum)
	require.NoError(t, err)
	_, tblIdx, err := objects.ReadBlockFrom(bytes.NewReader(b))
	require.NoError(t, err)
	return tbl, tblIdx
}

func TestDiffTables(t *testing.T) {
	db := kvtestutils.NewMockStore(false)
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
					PK:     hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"),
					Sum:    hexToBytes(t, "d5a84d255207bd4bce4a29ca5c82458f"),
					OldSum: hexToBytes(t, "ff1f6a4585b59abe0c74aa78510be549"),
					Row:    1,
					OldRow: 1,
				},
				{
					PK:  hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
					Sum: hexToBytes(t, "776beabc377528a964029835c5387e86"),
					Row: 2,
				},
				{
					PK:     hexToBytes(t, "c5e86ba7d7653eec345ae9b6d77ab0cc"),
					OldSum: hexToBytes(t, "62c10aeb1a926976d3a1775bc22908c0"),
					OldRow: 2,
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
					PK:     hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"),
					Sum:    hexToBytes(t, "d5a84d255207bd4bce4a29ca5c82458f"),
					OldSum: hexToBytes(t, "ff1f6a4585b59abe0c74aa78510be549"),
					Row:    1,
					OldRow: 1,
				},
				{
					PK:  hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
					Sum: hexToBytes(t, "776beabc377528a964029835c5387e86"),
					Row: 2,
				},
				{
					PK:     hexToBytes(t, "c5e86ba7d7653eec345ae9b6d77ab0cc"),
					OldSum: hexToBytes(t, "62c10aeb1a926976d3a1775bc22908c0"),
					OldRow: 2,
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
					PK:     hexToBytes(t, "00259da5fe4e202b974d64009944ccfe"),
					Sum:    hexToBytes(t, "d5a84d255207bd4bce4a29ca5c82458f"),
					OldSum: hexToBytes(t, "d5a84d255207bd4bce4a29ca5c82458f"),
					Row:    1,
					OldRow: 1,
				},
				{
					PK:     hexToBytes(t, "e3c37d3bfd03aef8fac2794539e39160"),
					Sum:    hexToBytes(t, "776beabc377528a964029835c5387e86"),
					OldSum: hexToBytes(t, "776beabc377528a964029835c5387e86"),
					Row:    2,
					OldRow: 2,
				},
			},
		},
	}
	for i, c := range cases {
		errChan := make(chan error, 1000)
		tbl1, tblIdx1 := getTable(t, db, c.Sum1)
		tbl2, tblIdx2 := getTable(t, db, c.Sum2)
		diffChan, _ := DiffTables(db, tbl1, tbl2, tblIdx1, tblIdx2, 0, errChan)
		events := []*objects.Diff{}
		for e := range diffChan {
			events = append(events, e)
		}
		assert.Equal(t, c.Events, events, "case %d", i)
		close(errChan)
		_, ok := <-errChan
		assert.False(t, ok)
	}
}

func ingestRawCSV(b *testing.B, db kvcommon.DB, fs kvfs.FileStore, rows [][]string) (*objects.Table, [][]string) {
	b.Helper()
	buf := bytes.NewBuffer(nil)
	require.NoError(b, csv.NewWriter(buf).WriteAll(rows))
	sum, err := ingest.IngestTable(db, io.NopCloser(bytes.NewReader(buf.Bytes())), "test.csv", nil, 0, 1, io.Discard)
	require.NoError(b, err)
	return getTable(b, db, sum)
}

func BenchmarkDiffRows(b *testing.B) {
	rawCSV1 := testutils.BuildRawCSV(12, b.N)
	rawCSV2 := testutils.ModifiedCSV(rawCSV1, 1)
	db := kvtestutils.NewMockStore(false)
	fs := kvtestutils.NewMockStore(false)
	tbl1, tblIdx1 := ingestRawCSV(b, db, fs, rawCSV1)
	tbl2, tblIdx2 := ingestRawCSV(b, db, fs, rawCSV2)
	errChan := make(chan error, 1000)
	b.ResetTimer()
	diffChan, _ := DiffTables(db, tbl1, tbl2, tblIdx1, tblIdx2, 0, errChan)
	for d := range diffChan {
		assert.NotNil(b, d)
	}
	_, ok := <-errChan
	assert.False(b, ok)
}
