package table

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
)

func TestBigStoreInsertRow(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	columns := []string{"a", "b", "c"}
	pk := []uint32{0}
	var seed uint64 = 0

	builder := NewBuilder(db, fs, columns, pk, seed, 1)
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
