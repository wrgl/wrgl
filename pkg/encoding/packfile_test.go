package encoding

import (
	"bytes"
	"io"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func TestPackfileWriter(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	w := NewPackfileWriter(buf)
	commit := testutils.SecureRandomBytes(1000)
	table := testutils.SecureRandomBytes(4000)
	row := testutils.SecureRandomBytes(256)
	require.NoError(t, w.WriteObject(ObjectCommit, commit))
	require.NoError(t, w.WriteObject(ObjectTable, table))
	require.NoError(t, w.WriteObject(ObjectRow, row))

	spew.Dump(buf.Bytes()[:64])

	r := NewPackfileReader(bytes.NewReader(buf.Bytes()))
	typ, b, err := r.ReadObject()
	require.NoError(t, err)
	assert.Equal(t, ObjectCommit, typ)
	assert.Equal(t, commit, b)
	typ, b, err = r.ReadObject()
	require.NoError(t, err)
	assert.Equal(t, ObjectTable, typ)
	assert.Equal(t, table, b)
	typ, b, err = r.ReadObject()
	require.NoError(t, err)
	assert.Equal(t, ObjectRow, typ)
	assert.Equal(t, row, b)
	_, _, err = r.ReadObject()
	assert.Equal(t, io.EOF, err)
}
