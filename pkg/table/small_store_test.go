package table

import (
	"encoding/hex"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
)

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

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestSmallStoreInsertRow(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	columns := []string{"a", "b", "c"}
	pk := []uint32{0}
	var seed uint64 = 0

	builder := NewBuilder(db, fs, columns, pk, seed, 0)
	pkh1 := mustDecodeHex(t, "52fdfc072182654f163f5f0f9a621d72")
	rh1 := mustDecodeHex(t, "2f8282cbe2f9696f3144c0aa4ced56db")
	pkh2 := mustDecodeHex(t, "85fbe72b6064289004a531f967898df5")
	rh2 := mustDecodeHex(t, "e2807d9c1dce26af00ca81d4fe11c23e")
	enc := objects.NewStrListEncoder()
	err := builder.InsertRow(0, pkh1, rh1, enc.Encode([]string{"q", "w", "e"}))
	require.NoError(t, err)
	err = builder.InsertRow(1, pkh2, rh2, enc.Encode([]string{"d", "e", "f"}))
	require.NoError(t, err)
	sum, err := builder.SaveTable()
	require.NoError(t, err)
	assert.Equal(t, "942f91d27534c9795d75b7df7255825a", hex.EncodeToString(sum))

	ts, err := ReadTable(db, fs, sum)
	require.NoError(t, err)
	assert.Equal(t, columns, ts.Columns())
	assert.Equal(t, []string{"a"}, ts.PrimaryKey())
	assert.Equal(t, pk, ts.PrimaryKeyIndices())
	assert.Equal(t, 2, ts.NumRows())
	rowHash, ok := ts.GetRowHash(pkh1)
	assert.True(t, ok)
	assert.Equal(t, rh1, rowHash)
	rowHash, ok = ts.GetRowHash(pkh2)
	assert.True(t, ok)
	assert.Equal(t, rh2, rowHash)
	_, ok = ts.GetRowHash(testutils.SecureRandomBytes(16))
	assert.False(t, ok)

	err = DeleteTable(db, fs, sum)
	require.NoError(t, err)
	_, err = ReadTable(db, fs, sum)
	assert.Equal(t, kv.KeyNotFoundError, err)
}

func createStore(t *testing.T, db kv.Store, fs kv.FileStore, pk []uint32, rows []string, bigStoreThreshold int) (ts Store, sum []byte, pkHashes, rowHashes [][]byte) {
	t.Helper()
	columns := strings.Split(rows[0], ",")
	var seed uint64 = 0

	for i := 0; i < len(rows)-1; i++ {
		pkHashes = append(pkHashes, testutils.SecureRandomBytes(16))
		rowHashes = append(rowHashes, testutils.SecureRandomBytes(16))
	}

	builder := NewBuilder(db, fs, columns, pk, seed, bigStoreThreshold)
	for i, row := range rows[1:] {
		err := builder.InsertRow(i, pkHashes[i], rowHashes[i], []byte(row))
		require.NoError(t, err)
	}
	sum, err := builder.SaveTable()
	require.NoError(t, err)

	ts, err = ReadTable(db, fs, sum)
	require.NoError(t, err)
	return
}

func buildStore(t *testing.T, db kv.Store, fs kv.FileStore, bigStoreThreshold int) (ts Store, sum []byte) {
	rows := []string{}
	for i := 0; i < 4; i++ {
		row := []string{}
		for j := 0; j < 3; j++ {
			row = append(row, testutils.BrokenRandomLowerAlphaString(3))
		}
		rows = append(rows, strings.Join(row, ","))
	}
	ts, sum, _, _ = createStore(t, db, fs, []uint32{0}, rows, bigStoreThreshold)
	return
}

func TestGetAllSmallTableHashes(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)

	_, sum1 := buildStore(t, db, fs, 0)
	_, sum2 := buildStore(t, db, fs, 2)
	names := [][]byte{sum1, sum2}
	sort.Slice(names, func(i, j int) bool { return string(names[i]) < string(names[j]) })

	sl, err := GetAllTableHashes(db, fs)
	require.NoError(t, err)
	assert.Equal(t, names, sl)
}
