package diff

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/testutils"
)

func insertRow(t *testing.T, db kv.DB, k []byte, row []string) {
	t.Helper()
	b, err := encoding.EncodeStrings(row)
	require.NoError(t, err)
	err = table.SaveRow(db, k, b)
	require.NoError(t, err)
}

func TestInflate(t *testing.T) {
	db := kv.NewMockStore(false)
	row1Sum := testutils.SecureRandomBytes(16)
	row1Content := []string{"1", "2", "3"}
	insertRow(t, db, row1Sum, row1Content)
	row2Sum := testutils.SecureRandomBytes(16)
	row2Content := []string{"4", "5", "6"}
	insertRow(t, db, row2Sum, row2Content)

	diffs := []Diff{
		{Type: Init, Columns: []string{"a", "b", "c"}, OldColumns: []string{"a", "b", "d"}, PK: []string{"a"}},
		{Type: Progress, Progress: 100, Total: 1000},
		{Type: PrimaryKey, OldPK: []string{"b"}},
		{Type: RowAdd, Row: hex.EncodeToString(row1Sum)},
		{Type: RowRemove, Row: hex.EncodeToString(row2Sum)},
		{Type: RowChange, Row: hex.EncodeToString(row1Sum), OldRow: hex.EncodeToString(row2Sum)},
	}
	diffChan := make(chan Diff, len(diffs))
	for _, d := range diffs {
		diffChan <- d
	}
	close(diffChan)

	inflatedChan, errChan := Inflate(db, diffChan)
	inflatedSl := []InflatedDiff{}
	for d := range inflatedChan {
		inflatedSl = append(inflatedSl, d)
	}
	_, ok := <-errChan
	assert.False(t, ok)
	assert.Equal(t, []InflatedDiff{
		{Type: Init, Columns: []string{"a", "b", "c"}, OldColumns: []string{"a", "b", "d"}, PK: []string{"a"}},
		{Type: Progress, Progress: 100, Total: 1000},
		{Type: PrimaryKey, OldPK: []string{"b"}},
		{Type: RowAdd, Row: row1Content},
		{Type: RowRemove, Row: row2Content},
		{Type: RowChangeInit, RowChangeColumns: []*RowChangeColumn{
			{Name: "a"}, {Name: "b"}, {Name: "d", Removed: true}, {Name: "c", Added: true},
		}},
		{Type: RowChange, RowChangeRow: [][]string{
			{"1", "4"}, {"2", "5"}, {"6"}, {"3"},
		}},
	}, inflatedSl)
}
