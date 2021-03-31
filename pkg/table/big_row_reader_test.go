package table

import (
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/testutils"
)

func createBigStore(t *testing.T, db kv.Store, fs kv.FileStore, pk []int, rows []string) (ts *BigStore, sum string, pkHashes, rowHashes [][]byte) {
	t.Helper()
	columns := strings.Split(rows[0], ",")
	var seed uint64 = 0
	it, err := NewBigStore(db, fs, columns, pk, seed)
	ts = it.(*BigStore)
	require.NoError(t, err)

	for i := 0; i < len(rows)-1; i++ {
		pkHashes = append(pkHashes, testutils.SecureRandomBytes(16))
		rowHashes = append(rowHashes, testutils.SecureRandomBytes(16))
	}

	for i, row := range rows[1:] {
		err = ts.InsertRow(i, pkHashes[i], rowHashes[i], []byte(row))
		require.NoError(t, err)
	}
	sum, err = ts.Save()
	require.NoError(t, err)

	return
}

func createTempFileStore(t *testing.T) (fs kv.FileStore, cleanUp func()) {
	t.Helper()
	dir, err := ioutil.TempDir("", "file_store_test")
	require.NoError(t, err)
	cleanUp = func() { os.RemoveAll(dir) }
	fs = kv.NewFileStore(dir)
	return
}

func buildBigStore(t *testing.T, db kv.Store, fs kv.FileStore) (ts *BigStore, sum string) {
	rows := []string{}
	for i := 0; i < 4; i++ {
		row := []string{}
		for j := 0; j < 3; j++ {
			row = append(row, testutils.BrokenRandomLowerAlphaString(3))
		}
		rows = append(rows, strings.Join(row, ","))
	}
	ts, sum, _, _ = createBigStore(t, db, fs, []int{0}, rows)
	return
}

func TestBigRowReader(t *testing.T) {
	db := kv.NewMockStore(false)
	fs, cleanUp := createTempFileStore(t)
	defer cleanUp()

	ts, _, _, rowHashes := createBigStore(t, db, fs, []int{0}, []string{
		"a,b,c",
		"1,d,e",
		"2,f,g",
		"3,h,i",
		"4,j,k",
	})
	l, err := ts.NumRows()
	require.NoError(t, err)
	assert.Equal(t, 4, l)

	// test Read
	r, err := ts.NewRowReader()
	require.NoError(t, err)
	assertRowRead(t, r, rowHashes[0], []byte("1,d,e"))
	assertRowRead(t, r, rowHashes[1], []byte("2,f,g"))
	assertRowRead(t, r, rowHashes[2], []byte("3,h,i"))
	assertRowRead(t, r, rowHashes[3], []byte("4,j,k"))
	_, _, err = r.Read()
	assert.Equal(t, io.EOF, err)

	// test Seek
	r, err = ts.NewRowReader()
	require.NoError(t, err)
	off, err := r.Seek(2, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, 2, off)
	assertRowRead(t, r, rowHashes[2], []byte("3,h,i"))
	off, err = r.Seek(-2, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, 1, off)
	assertRowRead(t, r, rowHashes[1], []byte("2,f,g"))
	off, err = r.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, 3, off)
	assertRowRead(t, r, rowHashes[3], []byte("4,j,k"))

	// test ReadAt
	r, err = ts.NewRowReader()
	require.NoError(t, err)
	rh, rc, err := r.ReadAt(3)
	require.NoError(t, err)
	assert.Equal(t, rowHashes[3], rh)
	assert.Equal(t, []byte("4,j,k"), rc)
	assertRowRead(t, r, rowHashes[0], []byte("1,d,e"))
}
