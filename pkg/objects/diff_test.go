package objects

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

func TestDiffWriter(t *testing.T) {
	objs := []*Diff{
		{Type: DTPKChange, Columns: []string{"a", "b"}},
		{Type: DTColumnAdd, Columns: []string{"c", "d"}},
		{Type: DTColumnRem, Columns: []string{"e"}},
		{Type: DTRow, PK: testutils.SecureRandomBytes(16), Sum: testutils.SecureRandomBytes(16)},
		{Type: DTRow, PK: testutils.SecureRandomBytes(16), OldSum: testutils.SecureRandomBytes(16)},
		{Type: DTRow, PK: testutils.SecureRandomBytes(16), OldSum: testutils.SecureRandomBytes(16), Sum: testutils.SecureRandomBytes(16)},
	}
	buf := bytes.NewBuffer(nil)
	w := NewDiffWriter(buf)
	for _, obj := range objs {
		require.NoError(t, w.Write(obj))
	}

	r := NewDiffReader(bytes.NewReader(buf.Bytes()))
	for _, obj := range objs {
		d, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, obj, d)
	}
	_, err := r.Read()
	assert.Equal(t, io.EOF, err)
}
