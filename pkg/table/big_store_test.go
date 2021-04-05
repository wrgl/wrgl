package table

import (
	"encoding/hex"
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/testutils"
)

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestBigStoreInsertRow(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	columns := []string{"a", "b", "c"}
	pk := []uint32{0}
	var seed uint64 = 0

	ts, err := NewBigStore(db, fs, columns, pk, seed)
	require.NoError(t, err)
	assert.Equal(t, columns, ts.Columns())
	assert.Equal(t, []string{"a"}, ts.PrimaryKey())
	assert.Equal(t, pk, ts.PrimaryKeyIndices())

	pkh1 := mustDecodeHex(t, "52fdfc072182654f163f5f0f9a621d72")
	rh1 := mustDecodeHex(t, "2f8282cbe2f9696f3144c0aa4ced56db")
	pkh2 := mustDecodeHex(t, "85fbe72b6064289004a531f967898df5")
	rh2 := mustDecodeHex(t, "e2807d9c1dce26af00ca81d4fe11c23e")
	err = ts.InsertRow(0, pkh1, rh1, []byte("q,w,e"))
	require.NoError(t, err)
	err = ts.InsertRow(1, pkh2, rh2, []byte("d,e,f"))
	require.NoError(t, err)
	sum, err := ts.Save()
	require.NoError(t, err)
	assert.Equal(t, "1b1ac80225ed9b798909885195e420d4", sum)

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

	ts2, err := ReadBigStore(db, fs, seed, sum)
	require.NoError(t, err)
	rowHash, ok = ts2.GetRowHash(pkh1)
	assert.True(t, ok)
	assert.Equal(t, rh1, rowHash)
	rowHash, ok = ts2.GetRowHash(pkh2)
	assert.True(t, ok)
	assert.Equal(t, rh2, rowHash)

	err = DeleteBigStore(db, fs, sum)
	require.NoError(t, err)
	_, err = ReadBigStore(db, fs, seed, sum)
	assert.Equal(t, kv.KeyNotFoundError, err)
}

func TestBigStoreNewRowHashReader(t *testing.T) {
	db := kv.NewMockStore(false)
	dir, err := ioutil.TempDir("", "file_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	filestore := kv.NewFileStore(dir)
	columns := []string{"a", "b", "c"}
	pk := []uint32{0}
	var seed uint64 = 0
	ts, err := NewBigStore(db, filestore, columns, pk, seed)
	require.NoError(t, err)

	pkHashes := [][]byte{}
	rowHashes := [][]byte{}
	for i := 0; i < 4; i++ {
		pkHashes = append(pkHashes, testutils.SecureRandomBytes(16))
		rowHashes = append(rowHashes, testutils.SecureRandomBytes(16))
	}

	err = ts.InsertRow(0, pkHashes[0], rowHashes[0], []byte("a,b,c"))
	require.NoError(t, err)
	err = ts.InsertRow(2, pkHashes[1], rowHashes[1], []byte("d,e,f"))
	require.NoError(t, err)
	err = ts.InsertRow(3, pkHashes[2], rowHashes[2], []byte("g,h,j"))
	require.NoError(t, err)
	err = ts.InsertRow(1, pkHashes[3], rowHashes[3], []byte("l,m,n"))
	require.NoError(t, err)
	_, err = ts.Save()
	require.NoError(t, err)

	for i, c := range []struct {
		offset int
		size   int
		rows   [][2][]byte
	}{
		{
			0, 2,
			[][2][]byte{
				{pkHashes[0], rowHashes[0]},
				{pkHashes[3], rowHashes[3]},
			},
		},
		{
			2, 2,
			[][2][]byte{
				{pkHashes[1], rowHashes[1]},
				{pkHashes[2], rowHashes[2]},
			},
		},
		{
			4, 2,
			[][2][]byte{},
		},
		{
			0, 0,
			[][2][]byte{
				{pkHashes[0], rowHashes[0]},
				{pkHashes[3], rowHashes[3]},
				{pkHashes[1], rowHashes[1]},
				{pkHashes[2], rowHashes[2]},
			},
		},
	} {
		rhr, err := ts.NewRowHashReader(c.offset, c.size)
		require.NoError(t, err)
		assert.Equal(t, c.rows, readAllRowHashes(t, rhr), "case %d", i)
	}
}

func TestGetAllBigTableHashes(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)

	_, sum1 := buildBigStore(t, db, fs)
	_, sum2 := buildBigStore(t, db, fs)
	names := []string{sum1, sum2}
	sort.Strings(names)

	sl, err := GetAllBigTableHashes(db)
	require.NoError(t, err)
	assert.Equal(t, names, sl)
}
