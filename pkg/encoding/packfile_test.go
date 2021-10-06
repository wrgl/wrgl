// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package encoding

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/misc"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestPackfileWriter(t *testing.T) {
	buf := misc.NewBuffer(nil)
	w, err := NewPackfileWriter(buf)
	require.NoError(t, err)
	commit1 := testutils.SecureRandomBytes(166)
	commit2 := testutils.SecureRandomBytes(2047)
	table := testutils.SecureRandomBytes(4000)
	row := testutils.SecureRandomBytes(13)
	_, err = w.WriteObject(ObjectCommit, commit1)
	require.NoError(t, err)
	_, err = w.WriteObject(ObjectCommit, commit2)
	require.NoError(t, err)
	_, err = w.WriteObject(ObjectTable, table)
	require.NoError(t, err)
	_, err = w.WriteObject(ObjectBlock, row)
	require.NoError(t, err)

	r, err := NewPackfileReader(io.NopCloser(bytes.NewReader(buf.Bytes())))
	require.NoError(t, err)
	assert.Equal(t, 1, r.Version)
	typ, b, err := r.ReadObject()
	require.NoError(t, err)
	assert.Equal(t, ObjectCommit, typ)
	assert.Equal(t, commit1, b)
	typ, b, err = r.ReadObject()
	require.NoError(t, err)
	assert.Equal(t, ObjectCommit, typ)
	assert.Equal(t, commit2, b)
	typ, b, err = r.ReadObject()
	require.NoError(t, err)
	assert.Equal(t, ObjectTable, typ)
	assert.Equal(t, table, b)
	typ, b, err = r.ReadObject()
	require.NoError(t, err)
	assert.Equal(t, ObjectBlock, typ)
	assert.Equal(t, row, b)
	_, _, err = r.ReadObject()
	assert.Equal(t, io.EOF, err)
}

func TestPackfileReaderPutBackBytesIfNotAPackfile(t *testing.T) {
	b := []byte("notapackfile")
	_, err := NewPackfileReader(io.NopCloser(bytes.NewReader(b)))
	assert.Error(t, err, "not a packfile")
}
