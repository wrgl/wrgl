package objects

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/testutils"
)

func TestTableWriter(t *testing.T) {
	buf := misc.NewBuffer(nil)
	w := NewTableWriter(buf)
	columns := []string{"q", "w", "e", "r"}
	pk := []uint32{0}
	rows := [][]byte{
		testutils.SecureRandomBytes(32),
		testutils.SecureRandomBytes(32),
		testutils.SecureRandomBytes(32),
		testutils.SecureRandomBytes(32),
	}
	err := w.WriteMeta(columns, pk)
	require.NoError(t, err)
	for i := 3; i >= 0; i-- {
		err = w.WriteRowAt(rows[i], i)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	r, err := NewTableReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	table, err := r.ReadTable()
	require.NoError(t, err)
	assert.Equal(t, &Table{
		Columns: columns,
		PK:      pk,
		Rows:    rows,
	}, table)
}

func TestTableReader(t *testing.T) {
	buf := misc.NewBuffer(nil)
	w := NewTableWriter(buf)
	table := &Table{
		Columns: []string{"a", "b", "c", "d"},
		PK:      []uint32{0, 1},
		Rows: [][]byte{
			testutils.SecureRandomBytes(32),
			testutils.SecureRandomBytes(32),
			testutils.SecureRandomBytes(32),
		},
	}
	err := w.WriteTable(table)
	require.NoError(t, err)
	t.Logf("bytes %v", buf.Bytes())

	// test ReadTable
	r, err := NewTableReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	table2, err := r.ReadTable()
	require.NoError(t, err)
	assert.Equal(t, table, table2)
	assert.Equal(t, 3, r.RowsCount())
	assert.Equal(t, []string{"a", "b", "c", "d"}, r.Columns)
	assert.Equal(t, []uint32{0, 1}, r.PK)

	// test ReadRow
	r, err = NewTableReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		row, err := r.ReadRow()
		require.NoError(t, err)
		assert.Equal(t, table.Rows[i], row)
	}

	// test SeekStart
	off, err := r.SeekRow(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, 0, off)
	row, err := r.ReadRow()
	require.NoError(t, err)
	assert.Equal(t, table.Rows[0], row)

	// test SeekCurrent
	off, err = r.SeekRow(1, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, 2, off)
	row, err = r.ReadRow()
	require.NoError(t, err)
	assert.Equal(t, table.Rows[2], row)

	// test SeekEnd
	off, err = r.SeekRow(-2, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, 1, off)
	row, err = r.ReadRow()
	require.NoError(t, err)
	assert.Equal(t, table.Rows[1], row)

	// test ReadAt
	r, err = NewTableReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		row, err := r.ReadRowAt(i)
		require.NoError(t, err)
		assert.Equal(t, table.Rows[i], row)
	}
	row, err = r.ReadRow()
	require.NoError(t, err)
	assert.Equal(t, table.Rows[0], row)
}

func TestTableReaderParseError(t *testing.T) {
	buf := misc.NewBuffer([]byte("columns "))
	buf.Write(NewStrListEncoder().Encode([]string{"a", "b", "c"}))
	buf.Write([]byte("\nbad input"))
	_, err := NewTableReader(bytes.NewReader(buf.Bytes()))
	assert.Equal(t, `parse error at pos=25: expected string "\npk ", received "\nbad"`, err.Error())
}
