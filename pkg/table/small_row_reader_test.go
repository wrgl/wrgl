package table

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/testutils"
)

func assertRowRead(t *testing.T, r RowReader, rowHash, rowContent []byte) {
	t.Helper()
	rh, rc, err := r.Read()
	require.NoError(t, err)
	assert.Equal(t, rowHash, rh)
	assert.Equal(t, rowContent, rc)
}

func createHashSlice(sliceLen int) (result [][]byte) {
	for i := 0; i < sliceLen; i++ {
		result = append(result, testutils.SecureRandomBytes(16))
	}
	return
}

func TestSmallRowReader(t *testing.T) {
	db := kv.NewMockStore(false)
	columns := []string{"a", "b", "c"}
	pk := []uint32{0}
	var seed uint64 = 0
	ts := NewSmallStore(db, columns, pk, seed)

	pkHashes := createHashSlice(4)
	rowHashes := createHashSlice(4)

	err := ts.InsertRow(0, pkHashes[0], rowHashes[0], []byte("a,b,c"))
	require.NoError(t, err)
	err = ts.InsertRow(2, pkHashes[1], rowHashes[1], []byte("d,e,f"))
	require.NoError(t, err)
	err = ts.InsertRow(3, pkHashes[2], rowHashes[2], []byte("g,h,j"))
	require.NoError(t, err)
	err = ts.InsertRow(1, pkHashes[3], rowHashes[3], []byte("l,m,n"))
	require.NoError(t, err)

	// test Read
	r, err := ts.NewRowReader()
	require.NoError(t, err)
	assertRowRead(t, r, rowHashes[0], []byte("a,b,c"))
	assertRowRead(t, r, rowHashes[3], []byte("l,m,n"))
	assertRowRead(t, r, rowHashes[1], []byte("d,e,f"))
	assertRowRead(t, r, rowHashes[2], []byte("g,h,j"))
	_, _, err = r.Read()
	assert.Equal(t, io.EOF, err)

	// test Seek
	r, err = ts.NewRowReader()
	require.NoError(t, err)
	off, err := r.Seek(2, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, 2, off)
	assertRowRead(t, r, rowHashes[1], []byte("d,e,f"))
	off, err = r.Seek(-2, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, 1, off)
	assertRowRead(t, r, rowHashes[3], []byte("l,m,n"))
	off, err = r.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, 3, off)
	assertRowRead(t, r, rowHashes[2], []byte("g,h,j"))

	// test ReadAt
	r, err = ts.NewRowReader()
	require.NoError(t, err)
	rh, rc, err := r.ReadAt(3)
	require.NoError(t, err)
	assert.Equal(t, rowHashes[2], rh)
	assert.Equal(t, []byte("g,h,j"), rc)
	assertRowRead(t, r, rowHashes[0], []byte("a,b,c"))
}
