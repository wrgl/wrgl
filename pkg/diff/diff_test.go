package diff

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/testutils"
)

func TestDiffTables(t *testing.T) {
	cases := []struct {
		T1     table.Store
		T2     table.Store
		Events []Diff
	}{
		{
			table.NewMockStore([]string{"one", "two"}, []uint32{0}, nil),
			table.NewMockStore([]string{"one", "three"}, []uint32{1}, nil),
			[]Diff{
				{Type: Init, Columns: []string{"one", "two"}, OldColumns: []string{"one", "three"}, PK: []string{"one"}},
				{Type: PrimaryKey, OldPK: []string{"three"}},
			},
		},
		{
			table.NewMockStore([]string{"one", "two"}, []uint32{0}, nil),
			table.NewMockStore([]string{"one", "two"}, []uint32{0}, nil),
			[]Diff{
				{Type: Init, Columns: []string{"one", "two"}, OldColumns: []string{"one", "two"}, PK: []string{"one"}},
			},
		},
		{
			table.NewMockStore([]string{"one", "two"}, []uint32{0}, nil),
			table.NewMockStore([]string{"one", "two"}, []uint32{}, nil),
			[]Diff{
				{Type: Init, Columns: []string{"one", "two"}, OldColumns: []string{"one", "two"}, PK: []string{"one"}},
				{Type: PrimaryKey, OldPK: []string{}},
			},
		},
		{
			table.NewMockStore([]string{"a", "b", "c", "d"}, []uint32{0, 2}, nil),
			table.NewMockStore([]string{"a", "c", "d", "b"}, []uint32{0, 1}, nil),
			[]Diff{
				{Type: Init, Columns: []string{"a", "b", "c", "d"}, OldColumns: []string{"a", "c", "d", "b"}, PK: []string{"a", "c"}},
			},
		},
		{
			table.NewMockStore([]string{"a", "b", "c", "d"}, []uint32{0, 1}, nil),
			table.NewMockStore([]string{"b", "a", "c", "d"}, []uint32{0, 1}, nil),
			[]Diff{
				{Type: Init, Columns: []string{"a", "b", "c", "d"}, OldColumns: []string{"b", "a", "c", "d"}, PK: []string{"a", "b"}},
				{Type: PrimaryKey, OldPK: []string{"b", "a"}},
			},
		},
		{
			table.NewMockStore([]string{"a", "b"}, nil, [][2]string{
				{"abc", "123"},
				{"def", "456"},
				{"qwe", "234"},
			}),
			table.NewMockStore([]string{"a", "b", "c"}, nil, [][2]string{
				{"abc", "123"},
				{"def", "059"},
				{"asd", "789"},
			}),
			[]Diff{
				{Type: Init, Columns: []string{"a", "b"}, OldColumns: []string{"a", "b", "c"}, PK: []string{}},
			},
		},
		{
			table.NewMockStore([]string{"a", "b"}, nil, [][2]string{
				{"abc", "123"},
				{"def", "456"},
				{"qwe", "234"},
			}),
			table.NewMockStore([]string{"a", "b"}, nil, [][2]string{
				{"abc", "123"},
				{"def", "059"},
				{"asd", "789"},
			}),
			[]Diff{
				{Type: Init, Columns: []string{"a", "b"}, OldColumns: []string{"a", "b"}, PK: []string{}},
				{Type: RowChange, Row: "343536", OldRow: "303539"},
				{Type: RowAdd, Row: "323334"},
				{Type: RowRemove, Row: "373839"},
			},
		},
		{
			table.NewMockStore([]string{"a", "b"}, nil, [][2]string{
				{"abc", "123"},
				{"def", "456"},
				{"qwe", "234"},
			}),
			table.NewMockStore([]string{"a", "b"}, nil, [][2]string{
				{"abc", "123"},
				{"def", "456"},
				{"qwe", "234"},
			}),
			[]Diff{
				{Type: Init, Columns: []string{"a", "b"}, OldColumns: []string{"a", "b"}, PK: []string{}},
			},
		},
		{
			table.NewMockStore([]string{"a", "b"}, nil, [][2]string{
				{"abc", "123"},
				{"def", "456"},
				{"qwe", "234"},
			}),
			table.NewMockStore([]string{"a", "c"}, nil, [][2]string{
				{"abc", "123"},
				{"def", "456"},
				{"qwe", "234"},
			}),
			[]Diff{
				{Type: Init, Columns: []string{"a", "b"}, OldColumns: []string{"a", "c"}, PK: []string{}},
			},
		},

		{
			table.NewMockStore([]string{"one", "two"}, []uint32{0}, [][2]string{
				{"abc", "123"},
				{"def", "456"},
				{"qwe", "234"},
			}),
			table.NewMockStore([]string{"one", "two"}, []uint32{0}, [][2]string{
				{"abc", "123"},
				{"def", "059"},
				{"asd", "789"},
			}),
			[]Diff{
				{Type: Init, Columns: []string{"one", "two"}, OldColumns: []string{"one", "two"}, PK: []string{"one"}},
				{Type: RowChange, Row: "343536", OldRow: "303539"},
				{Type: RowAdd, Row: "323334"},
				{Type: RowRemove, Row: "373839"},
			},
		},
		{
			table.NewMockStore([]string{"one", "two", "three"}, []uint32{0}, [][2]string{
				{"abc", "123"},
				{"def", "456"},
			}),
			table.NewMockStore([]string{"one", "two"}, []uint32{0}, [][2]string{
				{"abc", "345"},
				{"def", "678"},
			}),
			[]Diff{
				{Type: Init, Columns: []string{"one", "two", "three"}, OldColumns: []string{"one", "two"}, PK: []string{"one"}},
				{Type: RowChange, Row: "313233", OldRow: "333435"},
				{Type: RowChange, Row: "343536", OldRow: "363738"},
			},
		},
		{
			table.NewMockStore([]string{"one", "two"}, []uint32{0}, [][2]string{
				{"abc", "123"},
				{"def", "456"},
				{"qwe", "234"},
			}),
			table.NewMockStore([]string{"one", "two"}, []uint32{0}, [][2]string{
				{"abc", "123"},
				{"def", "456"},
				{"qwe", "234"},
			}),
			[]Diff{
				{Type: Init, Columns: []string{"one", "two"}, OldColumns: []string{"one", "two"}, PK: []string{"one"}},
			},
		},
	}
	for i, c := range cases {
		errChan := make(chan error, 1000)
		diffChan := DiffTables(c.T1, c.T2, 100*time.Minute, errChan)
		events := []Diff{}
		for e := range diffChan {
			events = append(events, e)
		}
		assert.Equal(t, c.Events, events, "case %d", i)
		close(errChan)
		_, ok := <-errChan
		assert.False(t, ok)
	}
}

func ingestRawCSV(b *testing.B, db kv.DB, rows [][]string) table.Store {
	b.Helper()
	cols := rows[0]
	reader := testutils.RawCSVReader(rows[1:])
	store := table.NewSmallStore(db, cols, []uint32{}, 0)
	_, err := ingest.Ingest(0, 1, reader, []uint32{}, store, io.Discard)
	require.NoError(b, err)
	return store
}

func BenchmarkDiffRows(b *testing.B) {
	rawCSV1 := testutils.BuildRawCSV(12, b.N)
	rawCSV2 := testutils.ModifiedCSV(rawCSV1, 1)
	db := kv.NewMockStore(false)
	store1 := ingestRawCSV(b, db, rawCSV1)
	store2 := ingestRawCSV(b, db, rawCSV2)
	errChan := make(chan error, 1000)
	b.ResetTimer()
	diffChan := DiffTables(store1, store2, 100*time.Minute, errChan)
	for d := range diffChan {
		assert.NotNil(b, d)
	}
	_, ok := <-errChan
	assert.False(b, ok)
}
