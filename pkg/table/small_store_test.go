package table

import (
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/testutils"
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
	assert.Equal(t, pk, ts.PrimaryKeyIndices())

	pkh1 := mustDecodeHex(t, "52fdfc072182654f163f5f0f9a621d72")
	rh1 := mustDecodeHex(t, "2f8282cbe2f9696f3144c0aa4ced56db")
	pkh2 := mustDecodeHex(t, "85fbe72b6064289004a531f967898df5")
	rh2 := mustDecodeHex(t, "e2807d9c1dce26af00ca81d4fe11c23e")
	err := ts.InsertRow(0, pkh1, rh1, []byte("q,w,e"))
	require.NoError(t, err)
	err = ts.InsertRow(1, pkh2, rh2, []byte("d,e,f"))
	require.NoError(t, err)
	sum, err := ts.Save()
	require.NoError(t, err)
	assert.Equal(t, "df0167a307d078f008cbd26b59d03522", sum)
	n, err := ts.NumRows()
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	rowHash, ok := ts.GetRowHash(pkh1)
	assert.True(t, ok)
	assert.Equal(t, rh1, rowHash)
	rowHash, ok = ts.GetRowHash(pkh2)
	assert.True(t, ok)
	assert.Equal(t, rh2, rowHash)
	_, ok = ts.GetRowHash([]byte("non-existent"))
	assert.False(t, ok)

	ts2, err := ReadSmallStore(db, seed, sum)
	require.NoError(t, err)
	rowHash, ok = ts2.GetRowHash(pkh1)
	assert.True(t, ok)
	assert.Equal(t, rh1, rowHash)
	rowHash, ok = ts2.GetRowHash(pkh2)
	assert.True(t, ok)
	assert.Equal(t, rh2, rowHash)

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
		offset int
		size   int
		rows   [][2]string
	}{
		{
			0, 2,
			[][2]string{
				{"a", "1"},
				{"d", "4"},
			},
		},
		{
			2, 2,
			[][2]string{
				{"b", "2"},
				{"c", "3"},
			},
		},
		{
			4, 2,
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
		},
	} {
		rhr, err := ts.NewRowHashReader(c.offset, c.size)
		require.NoError(t, err)
		assert.Equal(t, c.rows, readAllRowHashes(t, rhr))
	}
}

func createSmallStore(t *testing.T, db kv.Store, pk []int, rows []string) (ts *SmallStore, sum string, pkHashes, rowHashes [][]byte) {
	t.Helper()
	columns := strings.Split(rows[0], ",")
	var seed uint64 = 0
	it := NewSmallStore(db, columns, pk, seed)
	ts = it.(*SmallStore)

	for i := 0; i < len(rows)-1; i++ {
		pkHashes = append(pkHashes, testutils.SecureRandomBytes(16))
		rowHashes = append(rowHashes, testutils.SecureRandomBytes(16))
	}

	for i, row := range rows[1:] {
		err := ts.InsertRow(i, pkHashes[i], rowHashes[i], []byte(row))
		require.NoError(t, err)
	}
	sum, err := ts.Save()
	require.NoError(t, err)

	return
}

func buildSmallStore(t *testing.T, db kv.Store) (ts *SmallStore, sum string) {
	rows := []string{}
	for i := 0; i < 4; i++ {
		row := []string{}
		for j := 0; j < 3; j++ {
			row = append(row, testutils.BrokenRandomLowerAlphaString(3))
		}
		rows = append(rows, strings.Join(row, ","))
	}
	ts, sum, _, _ = createSmallStore(t, db, []int{0}, rows)
	return
}

func TestGetAllSmallTableHashes(t *testing.T) {
	db := kv.NewMockStore(false)

	_, sum1 := buildSmallStore(t, db)
	_, sum2 := buildSmallStore(t, db)
	names := []string{sum1, sum2}
	sort.Strings(names)

	sl, err := GetAllSmallTableHashes(db)
	require.NoError(t, err)
	assert.Equal(t, names, sl)
}
