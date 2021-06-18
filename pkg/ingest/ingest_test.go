// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package ingest

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
)

func readAllRowHashes(t *testing.T, reader table.RowHashReader) [][2]string {
	t.Helper()
	result := [][2]string{}
	for {
		pkh, rh, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		result = append(result, [2]string{hex.EncodeToString(pkh), hex.EncodeToString(rh)})
	}
	return result
}

func readAllRowContents(t *testing.T, reader table.RowReader) [][2]string {
	t.Helper()
	result := [][2]string{}
	for {
		pkh, rh, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		result = append(result, [2]string{hex.EncodeToString(pkh), string(rh)})
	}
	return result
}

func encodeStrList(sl []string) string {
	return string(objects.NewStrListEncoder().Encode(sl))
}

func TestIngest(t *testing.T) {
	cases := []struct {
		RawCSV                    []byte
		PrimaryKeys               []string
		ExpecteColumns            []string
		ExpectedPrimaryKeyIndices []int
		ExpectedRows              [][2]string
		ExpectedRowContent        [][2]string
		ExpectedError             error
	}{
		{
			RawCSV: []byte(strings.Join([]string{
				"name,age",
				"Alex,23",
				"Tom,24",
			}, "\n")),
			PrimaryKeys:    []string{"name"},
			ExpecteColumns: []string{"name", "age"},
			ExpectedRows: [][2]string{
				{"bb3eae9fd97b07e0e68397376cb3f91b", "cb09971aee2e66976aee18a0327ca566"},
				{"03fe30af7e8206d446f19436e9f41721", "5b9dca9219b276aeb09a0ee8ce84b15c"},
			},
			ExpectedRowContent: [][2]string{
				{"cb09971aee2e66976aee18a0327ca566", encodeStrList([]string{"Alex", "23"})},
				{"5b9dca9219b276aeb09a0ee8ce84b15c", encodeStrList([]string{"Tom", "24"})},
			},
			ExpectedError: nil,
		},
		{
			RawCSV: []byte(strings.Join([]string{
				"name,age",
				"Alex,23",
				"Tom,24",
			}, "\n")),
			ExpecteColumns: []string{"name", "age"},
			ExpectedRows: [][2]string{
				{"cb09971aee2e66976aee18a0327ca566", "cb09971aee2e66976aee18a0327ca566"},
				{"5b9dca9219b276aeb09a0ee8ce84b15c", "5b9dca9219b276aeb09a0ee8ce84b15c"},
			},
			ExpectedRowContent: [][2]string{
				{"cb09971aee2e66976aee18a0327ca566", encodeStrList([]string{"Alex", "23"})},
				{"5b9dca9219b276aeb09a0ee8ce84b15c", encodeStrList([]string{"Tom", "24"})},
			},
			ExpectedError: nil,
		},
		{
			RawCSV: []byte(strings.Join([]string{
				"one,two",
				"1,2",
			}, "\n")),
			PrimaryKeys:   []string{"three"},
			ExpectedError: fmt.Errorf(`slice.KeyIndices: key "three" not found in string slice`),
		},
		{
			RawCSV: []byte(strings.Join([]string{
				"one,two",
				"1,2",
				"1,3",
			}, "\n")),
			PrimaryKeys:    []string{"one"},
			ExpecteColumns: []string{"one", "two"},
			ExpectedRows: [][2]string{
				{"fd1c9513cc47feaf59fa9b76008f2521", "212684f52418cf420d72bcb96684259e"},
				{"fd1c9513cc47feaf59fa9b76008f2521", "f5f678067f0893090ab139630617771a"},
			},
			ExpectedRowContent: [][2]string{
				{"212684f52418cf420d72bcb96684259e", encodeStrList([]string{"1", "2"})},
				{"f5f678067f0893090ab139630617771a", encodeStrList([]string{"1", "3"})},
			},
			ExpectedError: nil,
		},
	}
	for i, c := range cases {
		buf := bytes.NewBuffer(c.RawCSV)
		db := kv.NewMockStore(false)
		fs := kv.NewMockStore(false)
		var seed uint64 = 0
		reader, columns, pk, err := ReadColumns(buf, c.PrimaryKeys)
		if err != nil {
			assert.Equal(t, c.ExpectedError, err, "case %d", i)
			continue
		}
		tb := table.NewBuilder(db, fs, columns, pk, seed, 0)
		sum, err := NewIngestor(tb, seed, pk, 1, io.Discard).
			ReadRowsFromCSVReader(reader).
			Ingest()
		if c.ExpectedError != nil {
			assert.Equal(t, c.ExpectedError, err, "case %d", i)
		} else {
			ts2, err := table.ReadTable(db, fs, sum)
			require.NoError(t, err)
			assert.Equal(t, c.ExpecteColumns, ts2.Columns(), "case %d", i)
			if c.PrimaryKeys == nil {
				assert.Empty(t, ts2.PrimaryKey(), "case %d", i)
			} else {
				assert.Equal(t, c.PrimaryKeys, ts2.PrimaryKey(), "case %d", i)
			}
			rhr := ts2.NewRowHashReader(0, 0)
			assert.Equal(t, c.ExpectedRows, readAllRowHashes(t, rhr), "case %d", i)
			rr := ts2.NewRowReader()
			assert.Equal(t, c.ExpectedRowContent, readAllRowContents(t, rr), "case %d", i)
		}
	}
}
