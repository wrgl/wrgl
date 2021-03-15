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
				{"ca68dab956b727df49264d1515ac90b9", "68bbfa1cf846c55636de2be4b66a6177"},
				{"bd06ebb25408380e34744c9d260eaae8", "954ff963c30b7b3cc589ac6b158ff11a"},
			},
			ExpectedRowContent: [][2]string{
				{"68bbfa1cf846c55636de2be4b66a6177", "Alex,23\n"},
				{"954ff963c30b7b3cc589ac6b158ff11a", "Tom,24\n"},
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
				{"68bbfa1cf846c55636de2be4b66a6177", "68bbfa1cf846c55636de2be4b66a6177"},
				{"954ff963c30b7b3cc589ac6b158ff11a", "954ff963c30b7b3cc589ac6b158ff11a"},
			},
			ExpectedRowContent: [][2]string{
				{"68bbfa1cf846c55636de2be4b66a6177", "Alex,23\n"},
				{"954ff963c30b7b3cc589ac6b158ff11a", "Tom,24\n"},
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
				{"71428b5a7a46150b829b22566bf92583", "390e3427c9ba3423d4461c905e4e93fa"},
				{"71428b5a7a46150b829b22566bf92583", "688833ce2881f5a9d0cc8a6008d13347"},
			},
			ExpectedRowContent: [][2]string{
				{"390e3427c9ba3423d4461c905e4e93fa", "1,2\n"},
				{"688833ce2881f5a9d0cc8a6008d13347", "1,3\n"},
			},
			ExpectedError: nil,
		},
	}
	for _, c := range cases {
		buf := bytes.NewBuffer(c.RawCSV)
		db := kv.NewMockStore(false)
		var seed uint64 = 0
		reader, columns, pk, err := ReadColumns(buf, c.PrimaryKeys)
		if err != nil {
			assert.Equal(t, c.ExpectedError, err)
			continue
		}
		ts := table.NewSmallStore(db, columns, pk, seed)
		sum, err := Ingest(seed, 1, reader, pk, ts)
		if c.ExpectedError != nil {
			assert.Equal(t, c.ExpectedError, err)
		} else {
			ts2, err := table.ReadSmallStore(db, seed, sum)
			require.NoError(t, err)
			assert.Equal(t, c.ExpecteColumns, ts2.Columns())
			if c.PrimaryKeys == nil {
				assert.Empty(t, ts2.PrimaryKey())
			} else {
				assert.Equal(t, c.PrimaryKeys, ts2.PrimaryKey())
			}
			rhr, err := ts2.NewRowHashReader(0, 0)
			require.NoError(t, err)
			assert.Equal(t, c.ExpectedRows, readAllRowHashes(t, rhr))
			rr, err := ts2.NewRowReader(0, 0)
			require.NoError(t, err)
			assert.Equal(t, c.ExpectedRowContent, readAllRowContents(t, rr))
		}
	}
}
