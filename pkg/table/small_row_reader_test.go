package table

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
)

func assertRowRead(t *testing.T, r RowReader, rowHash, rowContent []byte) {
	t.Helper()
	rh, rc, err := r.Read()
	require.NoError(t, err)
	assert.Equal(t, rowHash, rh)
	assert.Equal(t, rowContent, rc)
}

func TestSmallRowReader(t *testing.T) {
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

	// test Read
	r, err := ts.NewRowReader()
	require.NoError(t, err)
	assertRowRead(t, r, []byte("1"), []byte("a,b,c"))
	assertRowRead(t, r, []byte("4"), []byte("l,m,n"))
	assertRowRead(t, r, []byte("2"), []byte("d,e,f"))
	assertRowRead(t, r, []byte("3"), []byte("g,h,j"))
	_, _, err = r.Read()
	assert.Equal(t, io.EOF, err)

	// test Seek
	r, err = ts.NewRowReader()
	require.NoError(t, err)
	off, err := r.Seek(2, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, 2, off)
	assertRowRead(t, r, []byte("2"), []byte("d,e,f"))
	off, err = r.Seek(-2, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, 1, off)
	assertRowRead(t, r, []byte("4"), []byte("l,m,n"))
	off, err = r.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, 3, off)
	assertRowRead(t, r, []byte("3"), []byte("g,h,j"))

	// test ReadAt
	r, err = ts.NewRowReader()
	require.NoError(t, err)
	rh, rc, err := r.ReadAt(3)
	require.NoError(t, err)
	assert.Equal(t, []byte("3"), rh)
	assert.Equal(t, []byte("g,h,j"), rc)
	assertRowRead(t, r, []byte("1"), []byte("a,b,c"))
}
