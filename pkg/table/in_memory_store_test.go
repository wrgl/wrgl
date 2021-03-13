package table

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
)

func TestInMemoryStoreInsertRow(t *testing.T) {
	db := kv.NewMockStore(false)
	columns := []string{"a", "b", "c"}
	pk := []int{0}
	var seed uint64 = 0

	ts := NewInMemoryStore(db, columns, pk, seed)
	assert.Equal(t, columns, ts.Columns())
	assert.Equal(t, []string{"a"}, ts.PrimaryKey())

	err := ts.InsertRow(0, []byte("bcd"), []byte("234"), []byte("q,w,e"))
	require.NoError(t, err)
	err = ts.InsertRow(1, []byte("abc"), []byte("123"), []byte("d,e,f"))
	require.NoError(t, err)
	assert.Equal(t, 2, ts.NumRows())
	rowHash, ok := ts.GetRowHash([]byte("abc"))
	assert.True(t, ok)
	assert.Equal(t, []byte("123"), rowHash)
	rowHash, ok = ts.GetRowHash([]byte("bcd"))
	assert.True(t, ok)
	assert.Equal(t, []byte("234"), rowHash)
	_, ok = ts.GetRowHash([]byte("non-existent"))
	assert.False(t, ok)
	sum, err := ts.Save()
	require.NoError(t, err)
	assert.Equal(t, "b1705f1896bb3a2b0e0b0efa6d25c832", sum)

	ts2, err := ReadInMemoryStore(db, seed, sum)
	require.NoError(t, err)
	rowHash, ok = ts2.GetRowHash([]byte("abc"))
	assert.True(t, ok)
	assert.Equal(t, []byte("123"), rowHash)
	rowHash, ok = ts2.GetRowHash([]byte("bcd"))
	assert.True(t, ok)
	assert.Equal(t, []byte("234"), rowHash)

	err = DeleteInMemoryStore(db, sum)
	require.NoError(t, err)
	_, err = ReadInMemoryStore(db, seed, sum)
	assert.Equal(t, kv.KeyNotFoundError, err)
}

func readAllRowHashes(t *testing.T, reader RowHashReader) [][2][]byte {
	t.Helper()
	result := [][2][]byte{}
	for {
		pkh, rh, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		result = append(result, [2][]byte{pkh, rh})
	}
	return result
}

func TestInMemoryStoreNewRowHashReader(t *testing.T) {
	db := kv.NewMockStore(false)
	columns := []string{"a", "b", "c"}
	pk := []int{0}
	var seed uint64 = 0
	ts := NewInMemoryStore(db, columns, pk, seed)

	err := ts.InsertRow(0, []byte("a"), []byte("1"), []byte("a,b,c"))
	require.NoError(t, err)
	err = ts.InsertRow(2, []byte("b"), []byte("2"), []byte("d,e,f"))
	require.NoError(t, err)
	err = ts.InsertRow(3, []byte("c"), []byte("3"), []byte("g,h,j"))
	require.NoError(t, err)
	err = ts.InsertRow(1, []byte("d"), []byte("4"), []byte("l,m,n"))
	require.NoError(t, err)

	rhReader := ts.NewRowHashReader(0, 2)
	sl := readAllRowHashes(t, rhReader)
	assert.Equal(t, [][2][]byte{
		{[]byte("a"), []byte("1")},
		{[]byte("d"), []byte("4")},
	}, sl)

	rhReader = ts.NewRowHashReader(2, 2)
	sl = readAllRowHashes(t, rhReader)
	assert.Equal(t, [][2][]byte{
		{[]byte("b"), []byte("2")},
		{[]byte("c"), []byte("3")},
	}, sl)

	rhReader = ts.NewRowHashReader(0, 0)
	sl = readAllRowHashes(t, rhReader)
	assert.Equal(t, [][2][]byte{
		{[]byte("a"), []byte("1")},
		{[]byte("d"), []byte("4")},
		{[]byte("b"), []byte("2")},
		{[]byte("c"), []byte("3")},
	}, sl)
}
