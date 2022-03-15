// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objline

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/encoding"
	"github.com/wrgl/wrgl/pkg/misc"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestWriteField(t *testing.T) {
	b := testutils.SecureRandomBytes(16)
	s := testutils.BrokenRandomAlphaNumericString(20)
	var u uint32 = 12345678
	buf := bytes.NewBuffer(nil)
	br := misc.NewBuffer(nil)

	n1, err := WriteField(buf, br, "bytes", WriteBytes(b))
	require.NoError(t, err)
	n2, err := WriteField(buf, br, "string", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
		return WriteString(w, buf, s)
	})
	require.NoError(t, err)
	n3, err := WriteField(buf, br, "uint32", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
		return WriteUint32(w, buf, u)
	})
	require.NoError(t, err)

	p := encoding.NewParser(bytes.NewReader(buf.Bytes()))
	b2 := make([]byte, 16)
	m1, err := ReadField(p, "bytes", ReadBytes(b2))
	require.NoError(t, err)
	assert.Equal(t, n1, m1)
	assert.Equal(t, b, b2)
	var s2 string
	m2, err := ReadField(p, "string", func(p *encoding.Parser) (int64, error) {
		return ReadString(p, &s2)
	})
	require.NoError(t, err)
	assert.Equal(t, n2, m2)
	assert.Equal(t, s, s2)
	var u2 uint32
	m3, err := ReadField(p, "uint32", func(p *encoding.Parser) (int64, error) {
		return ReadUint32(p, &u2)
	})
	require.NoError(t, err)
	assert.Equal(t, n3, m3)
	assert.Equal(t, u, u2)
}
