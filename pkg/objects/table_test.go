package objects

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func TestTableWriter(t *testing.T) {
	buf := bytes.NewBufferString("")
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
	err := w.Write(table)
	require.NoError(t, err)

	r := NewTableReader(buf)
	table2, err := r.Read()
	require.NoError(t, err)
	assert.Equal(t, table, table2)
}

func TestTableReaderParseError(t *testing.T) {
	buf := bytes.NewBufferString("columns ")
	buf.Write(NewStrListEncoder().Encode([]string{"a", "b", "c"}))
	buf.WriteString("\nbad input")
	r := NewTableReader(buf)
	_, err := r.Read()
	assert.Equal(t, `parse error at pos=31: expected string "\npk ", received "\nbad"`, err.Error())
}
