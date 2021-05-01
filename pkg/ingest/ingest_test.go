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
				{"9510a7973269436b9627c409c77a5f2c", "1d2f16fe99e826a2157b0cede3bf4f10"},
				{"aa0f4193a207c8f27560292761e98f1c", "b8f03f03f6990def324123b661f487f7"},
			},
			ExpectedRowContent: [][2]string{
				{"1d2f16fe99e826a2157b0cede3bf4f10", encodeStrList([]string{"Alex", "23"})},
				{"b8f03f03f6990def324123b661f487f7", encodeStrList([]string{"Tom", "24"})},
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
				{"1d2f16fe99e826a2157b0cede3bf4f10", "1d2f16fe99e826a2157b0cede3bf4f10"},
				{"b8f03f03f6990def324123b661f487f7", "b8f03f03f6990def324123b661f487f7"},
			},
			ExpectedRowContent: [][2]string{
				{"1d2f16fe99e826a2157b0cede3bf4f10", encodeStrList([]string{"Alex", "23"})},
				{"b8f03f03f6990def324123b661f487f7", encodeStrList([]string{"Tom", "24"})},
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
				{"d0d1db2e777905a83d878c000698a566", "8503952f1bf1b485691a2f98f8b45821"},
				{"d0d1db2e777905a83d878c000698a566", "e48446853e98ea637ab1b72e992035ed"},
			},
			ExpectedRowContent: [][2]string{
				{"8503952f1bf1b485691a2f98f8b45821", encodeStrList([]string{"1", "2"})},
				{"e48446853e98ea637ab1b72e992035ed", encodeStrList([]string{"1", "3"})},
			},
			ExpectedError: nil,
		},
	}
	for i, c := range cases {
		buf := bytes.NewBuffer(c.RawCSV)
		db := kv.NewMockStore(false)
		var seed uint64 = 0
		reader, columns, pk, err := ReadColumns(buf, c.PrimaryKeys)
		if err != nil {
			assert.Equal(t, c.ExpectedError, err, "case %d", i)
			continue
		}
		ts := table.NewSmallStore(db, columns, pk, seed)
		sum, err := Ingest(seed, 1, reader, pk, ts, io.Discard)
		if c.ExpectedError != nil {
			assert.Equal(t, c.ExpectedError, err, "case %d", i)
		} else {
			ts2, err := table.ReadSmallStore(db, seed, sum)
			require.NoError(t, err)
			assert.Equal(t, c.ExpecteColumns, ts2.Columns(), "case %d", i)
			if c.PrimaryKeys == nil {
				assert.Empty(t, ts2.PrimaryKey(), "case %d", i)
			} else {
				assert.Equal(t, c.PrimaryKeys, ts2.PrimaryKey(), "case %d", i)
			}
			rhr, err := ts2.NewRowHashReader(0, 0)
			require.NoError(t, err)
			assert.Equal(t, c.ExpectedRows, readAllRowHashes(t, rhr), "case %d", i)
			rr, err := ts2.NewRowReader()
			require.NoError(t, err)
			assert.Equal(t, c.ExpectedRowContent, readAllRowContents(t, rr), "case %d", i)
		}
	}
}
