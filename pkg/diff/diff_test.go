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
			table.NewMockStore([]string{"one", "two"}, []int{0}, nil),
			table.NewMockStore([]string{"one", "three"}, []int{1}, nil),
			[]Diff{
				{Type: Init, Columns: []string{"one", "two"}, OldColumns: []string{"one", "three"}, PK: []string{"one"}},
				{Type: PrimaryKey, OldPK: []string{"three"}},
			},
		},
		{
			table.NewMockStore([]string{"one", "two"}, []int{0}, nil),
			table.NewMockStore([]string{"one", "two"}, []int{0}, nil),
			[]Diff{
				{Type: Init, Columns: []string{"one", "two"}, OldColumns: []string{"one", "two"}, PK: []string{"one"}},
			},
		},
		{
			table.NewMockStore([]string{"one", "two"}, []int{0}, nil),
			table.NewMockStore([]string{"one", "two"}, []int{}, nil),
			[]Diff{
				{Type: Init, Columns: []string{"one", "two"}, OldColumns: []string{"one", "two"}, PK: []string{"one"}},
				{Type: PrimaryKey, OldPK: []string{}},
			},
		},
		{
			table.NewMockStore([]string{"a", "b", "c", "d"}, []int{0, 2}, nil),
			table.NewMockStore([]string{"a", "c", "d", "b"}, []int{0, 1}, nil),
			[]Diff{
				{Type: Init, Columns: []string{"a", "b", "c", "d"}, OldColumns: []string{"a", "c", "d", "b"}, PK: []string{"a", "c"}},
			},
		},
		{
			table.NewMockStore([]string{"a", "b", "c", "d"}, []int{0, 1}, nil),
			table.NewMockStore([]string{"b", "a", "c", "d"}, []int{0, 1}, nil),
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
			table.NewMockStore([]string{"one", "two"}, []int{0}, [][2]string{
				{"abc", "123"},
				{"def", "456"},
				{"qwe", "234"},
			}),
			table.NewMockStore([]string{"one", "two"}, []int{0}, [][2]string{
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
			table.NewMockStore([]string{"one", "two", "three"}, []int{0}, [][2]string{
				{"abc", "123"},
				{"def", "456"},
			}),
			table.NewMockStore([]string{"one", "two"}, []int{0}, [][2]string{
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
			table.NewMockStore([]string{"one", "two"}, []int{0}, [][2]string{
				{"abc", "123"},
				{"def", "456"},
				{"qwe", "234"},
			}),
			table.NewMockStore([]string{"one", "two"}, []int{0}, [][2]string{
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
		diffChan := make(chan Diff, 1000)
		err := DiffTables(c.T1, c.T2, diffChan, 100*time.Minute)
		require.NoError(t, err)
		events := []Diff{}
		close(diffChan)
		for e := range diffChan {
			events = append(events, e)
		}
		assert.Equal(t, c.Events, events, "case %d", i)
	}
}

func ingestRawCSV(b *testing.B, db kv.DB, rows [][]string) table.Store {
	b.Helper()
	cols := rows[0]
	reader := testutils.RawCSVReader(rows[1:])
	store := table.NewSmallStore(db, cols, []int{}, 0)
	_, err := ingest.Ingest(0, 1, reader, []int{}, store, io.Discard)
	require.NoError(b, err)
	return store
}

func BenchmarkDiffRows(b *testing.B) {
	rawCSV1 := testutils.BuildRawCSV(12, b.N)
	rawCSV2 := testutils.ModifiedCSV(rawCSV1, 1)
	db := kv.NewMockStore(false)
	store1 := ingestRawCSV(b, db, rawCSV1)
	store2 := ingestRawCSV(b, db, rawCSV2)
	diffChan := make(chan Diff, b.N)
	defer close(diffChan)
	b.ResetTimer()
	require.NoError(b, DiffTables(store1, store2, diffChan, 100*time.Minute))
}
