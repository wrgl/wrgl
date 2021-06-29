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
	return tbl, tblIdx
}

func TestDiffTables(t *testing.T) {
	db := kvtestutils.NewMockStore(false)
	cases := []struct {
		Sum1, Sum2 []byte
		Events     []objects.Diff
	}{
		{
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,q",
				"2,a",
				"3,z",
			}, []uint32{0}),
			factory.BuildTable(t, db, []string{
				"a,b,c",
				"1,q,w",
				"2,a,s",
				"3,z,x",
			}, []uint32{0}),
			[]objects.Diff{},
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
				"2,a",
				"3,z",
			}, []uint32{0}),
			[]objects.Diff{},
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
			[]objects.Diff{},
		},
		{
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,q",
				"2,a",
				"3,z",
			}, nil),
			factory.BuildTable(t, db, []string{
				"a,c",
				"1,q",
				"2,a",
				"3,z",
			}, nil),
			[]objects.Diff{},
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
			[]objects.Diff{
				{PK: []byte("def"), Sum: []byte("456"), OldSum: []byte("059")},
				{PK: []byte("qwe"), Sum: []byte("234")},
				{PK: []byte("asd"), OldSum: []byte("789")},
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
			[]objects.Diff{
				{PK: []byte("def"), Sum: []byte("456"), OldSum: []byte("059")},
				{PK: []byte("qwe"), Sum: []byte("234")},
				{PK: []byte("asd"), OldSum: []byte("789")},
			},
		},
		{
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,q",
				"2,a",
			}, nil),
			factory.BuildTable(t, db, []string{
				"a,b",
				"1,w",
				"2,s",
			}, nil),
			[]objects.Diff{
				{PK: []byte("abc"), Sum: []byte("123"), OldSum: []byte("345")},
				{PK: []byte("def"), Sum: []byte("456"), OldSum: []byte("678")},
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
			[]objects.Diff{
				{PK: []byte("abc"), Sum: []byte("123"), OldSum: []byte("123")},
				{PK: []byte("def"), Sum: []byte("456"), OldSum: []byte("456")},
				{PK: []byte("qwe"), Sum: []byte("234"), OldSum: []byte("234")},
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

// func TestDiffTablesEmitRowChangeWhenPKDiffer(t *testing.T) {
// 	db := kvtestutils.NewMockStore(false)
// 	sum1 := factory.BuildTable(t, db, []string{
// 		"a,b",
// 		"1,q",
// 		"2,a",
// 	}, []uint32{0})
// 	sum2 := factory.BuildTable(t, db, []string{
// 		"a,b",
// 		"3,z",
// 		"4,x",
// 	}, []uint32{0})
// 	tbl1, tblIdx1 := getTable(t, db, sum1)
// 	tbl2, tblIdx2 := getTable(t, db, sum2)
// 	errChan := make(chan error, 1000)
// 	diffChan, _ := DiffTables(ts1, ts2, 0, errChan, true)
// 	events := []objects.Diff{}
// 	for e := range diffChan {
// 		events = append(events, e)
// 	}
// 	assert.Equal(t, []objects.Diff{
// 		{PK: []byte("abc"), Sum: []byte("123")},
// 		{PK: []byte("def"), Sum: []byte("456")},
// 		{PK: []byte("wer"), OldSum: []byte("321")},
// 		{PK: []byte("sdf"), OldSum: []byte("432")},
// 	}, events)
// }

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
