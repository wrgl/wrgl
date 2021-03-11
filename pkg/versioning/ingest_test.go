package versioning

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wrgl/core/pkg/testutils"
)

func hexify(src []KeyHash) map[string]string {
	res := map[string]string{}
	for _, r := range src {
		res[hex.EncodeToString([]byte(r.K))] = hex.EncodeToString(r.V)
	}
	return res
}

func hexifyKey(src map[string][]byte) map[string]string {
	res := map[string]string{}
	for k, v := range src {
		res[hex.EncodeToString([]byte(k))] = string(v)
	}
	return res
}

func TestIngestCSV(t *testing.T) {
	table := []struct {
		RawCSV                    []byte
		PrimaryKeys               []string
		ExpecteColumns            []string
		ExpectedPrimaryKeyIndices []int
		ExpectedRows              map[string]string
		ExpectedRowContent        map[string]string
		ExpectedError             error
	}{
		{
			RawCSV: []byte(strings.Join([]string{
				"name,age",
				"Alex,23",
				"Tom,24",
			}, "\n")),
			PrimaryKeys:               []string{"name"},
			ExpecteColumns:            []string{"name", "age"},
			ExpectedPrimaryKeyIndices: []int{0},
			ExpectedRows: map[string]string{
				"22cc2b9e3b7c5e886be47ecc4da7f4f8": "930a44056b13fabfacfa2153544ec9d6",
				"e8944aec1ebf0e049b98efc5a713719a": "b50c41c21ae559a3893ffde0ddfd9cbd",
			},
			ExpectedRowContent: map[string]string{
				"930a44056b13fabfacfa2153544ec9d6": "Tom,24",
				"b50c41c21ae559a3893ffde0ddfd9cbd": "Alex,23",
			},
			ExpectedError: nil,
		},
		{
			RawCSV: []byte(strings.Join([]string{
				"name,age",
				"Alex,23",
				"Tom,24",
			}, "\n")),
			ExpecteColumns:            []string{"name", "age"},
			ExpectedPrimaryKeyIndices: []int{},
			ExpectedRows: map[string]string{
				"930a44056b13fabfacfa2153544ec9d6": "930a44056b13fabfacfa2153544ec9d6",
				"b50c41c21ae559a3893ffde0ddfd9cbd": "b50c41c21ae559a3893ffde0ddfd9cbd",
			},
			ExpectedRowContent: map[string]string{
				"930a44056b13fabfacfa2153544ec9d6": "Tom,24",
				"b50c41c21ae559a3893ffde0ddfd9cbd": "Alex,23",
			},
			ExpectedError: nil,
		},
		{
			RawCSV: []byte(strings.Join([]string{
				"one,two",
				"1,2",
			}, "\n")),
			PrimaryKeys:   []string{"three"},
			ExpectedError: fmt.Errorf(`KeyIndices error: key "three" not found in string slice`),
		},
		{
			RawCSV: []byte(strings.Join([]string{
				"one,two",
				"1,2",
				"1,3",
			}, "\n")),
			PrimaryKeys:               []string{"one"},
			ExpecteColumns:            []string{"one", "two"},
			ExpectedPrimaryKeyIndices: []int{0},
			ExpectedRows: map[string]string{
				"ce89f91e16029b70fec1e8e83389fad5": "f5f3be73ecebe228d67468cdde7ec49a",
			},
			ExpectedRowContent: map[string]string{
				"f5f3be73ecebe228d67468cdde7ec49a": "1,3",
			},
			ExpectedError: nil,
		},
	}
	for _, c := range table {
		buf := bytes.NewBuffer([]byte{})
		_, err := buf.Write(c.RawCSV)
		if err != nil {
			panic(err)
		}
		reader := csv.NewReader(buf)
		ta, m, err := IngestCSV(reader, c.PrimaryKeys, uint64(0))
		assert.Equal(t, c.ExpectedError, err)
		if err == nil {
			assert.Equal(t, c.ExpecteColumns, ta.Columns, "Header does not match")
			assert.Equal(t, c.ExpectedPrimaryKeyIndices, ta.PrimaryKeys, "Primary key indices does not match")
			assert.Equal(t, c.ExpectedRows, hexify(ta.Rows), "Table rows do not match")
			assert.Equal(t, c.ExpectedRowContent, hexifyKey(m), "Row content does not match")
		}
	}
}

func BenchmarkIngestCSV(b *testing.B) {
	rawCSV := testutils.BuildRawCSV(12, b.N)
	reader := testutils.RawCSVReader(rawCSV)
	b.ResetTimer()
	_, _, err := IngestCSV(reader, []string{"id"}, uint64(0))
	if err != nil {
		panic(err)
	}
}
