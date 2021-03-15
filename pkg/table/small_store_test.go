package table

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
)

func readAllRowHashes(t *testing.T, reader RowHashReader) [][2]string {
	t.Helper()
	result := [][2]string{}
	for {
		pkh, rh, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		result = append(result, [2]string{string(pkh), string(rh)})
	}
	return result
}

func TestSmallStoreInsertRow(t *testing.T) {
	db := kv.NewMockStore(false)
	columns := []string{"a", "b", "c"}
	pk := []int{0}
	var seed uint64 = 0

	ts := NewSmallStore(db, columns, pk, seed)
	assert.Equal(t, columns, ts.Columns())
	assert.Equal(t, []string{"a"}, ts.PrimaryKey())

	err := ts.InsertRow(0, []byte("bcd"), []byte("234"), []byte("q,w,e"))
	require.NoError(t, err)
	err = ts.InsertRow(1, []byte("abc"), []byte("123"), []byte("d,e,f"))
	require.NoError(t, err)
	sum, err := ts.Save()
	require.NoError(t, err)
	assert.Equal(t, "b1705f1896bb3a2b0e0b0efa6d25c832", sum)
	n, err := ts.NumRows()
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	rowHash, ok := ts.GetRowHash([]byte("abc"))
	assert.True(t, ok)
	assert.Equal(t, []byte("123"), rowHash)
	rowHash, ok = ts.GetRowHash([]byte("bcd"))
	assert.True(t, ok)
	assert.Equal(t, []byte("234"), rowHash)
	_, ok = ts.GetRowHash([]byte("non-existent"))
	assert.False(t, ok)

	ts2, err := ReadSmallStore(db, seed, sum)
	require.NoError(t, err)
	rowHash, ok = ts2.GetRowHash([]byte("abc"))
	assert.True(t, ok)
	assert.Equal(t, []byte("123"), rowHash)
	rowHash, ok = ts2.GetRowHash([]byte("bcd"))
	assert.True(t, ok)
	assert.Equal(t, []byte("234"), rowHash)

	err = DeleteSmallStore(db, sum)
	require.NoError(t, err)
	_, err = ReadSmallStore(db, seed, sum)
	assert.Equal(t, kv.KeyNotFoundError, err)
}

func TestSmallStoreNewRowHashReader(t *testing.T) {
	db := kv.NewMockStore(false)
	columns := []string{"a", "b", "c"}
	pk := []int{0}
	var seed uint64 = 0
	ts := NewSmallStore(db, columns, pk, seed)

	err := ts.InsertRow(0, []byte("a"), []byte("1"), []byte("a,b,c"))
	require.NoError(t, err)
	err = ts.InsertRow(2, []byte("b"), []byte("2"), []byte("d,e,f"))
	require.NoError(t, err)
	err = ts.InsertRow(3, []byte("c"), []byte("3"), []byte("g,h,j"))
	require.NoError(t, err)
	err = ts.InsertRow(1, []byte("d"), []byte("4"), []byte("l,m,n"))
	require.NoError(t, err)

	for _, c := range []struct {
		offset      int
		size        int
		rows        [][2]string
		rowContents [][2]string
	}{
		{
			0, 2,
			[][2]string{
				{"a", "1"},
				{"d", "4"},
			},
			[][2]string{
				{"1", "a,b,c"},
				{"4", "l,m,n"},
			},
		},
		{
			2, 2,
			[][2]string{
				{"b", "2"},
				{"c", "3"},
			},
			[][2]string{
				{"2", "d,e,f"},
				{"3", "g,h,j"},
			},
		},
		{
			4, 2,
			[][2]string{},
			[][2]string{},
		},
		{
			0, 0,
			[][2]string{
				{"a", "1"},
				{"d", "4"},
				{"b", "2"},
				{"c", "3"},
			},
			[][2]string{
				{"1", "a,b,c"},
				{"4", "l,m,n"},
				{"2", "d,e,f"},
				{"3", "g,h,j"},
			},
		},
	} {
		rhr, err := ts.NewRowHashReader(c.offset, c.size)
		require.NoError(t, err)
		assert.Equal(t, c.rows, readAllRowHashes(t, rhr))
		rr, err := ts.NewRowReader(c.offset, c.size)
		require.NoError(t, err)
		assert.Equal(t, c.rowContents, readAllRowHashes(t, rr))
	}
}
