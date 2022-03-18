// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objline

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/encoding"
	"github.com/wrgl/wrgl/pkg/misc"
)

func TestWriteString(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	b := misc.NewBuffer(nil)
	for _, s := range []string{
		"", "abc", "123 sdf wer",
	} {
		buf.Reset()
		n, err := WriteString(buf, b, s)
		require.NoError(t, err)
		var s2 string
		m, err := ReadString(encoding.NewParser(bytes.NewReader(buf.Bytes())), &s2)
		if err != io.EOF {
			require.NoError(t, err)
		}
		assert.Equal(t, n, m)
		assert.Equal(t, s, s2)
	}
}

func TestWriteUint16(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	b := misc.NewBuffer(nil)
	for _, v := range []uint16{
		0, 10, 65534,
	} {
		buf.Reset()
		n, err := WriteUint16(buf, b, v)
		require.NoError(t, err)
		var v2 uint16
		m, err := ReadUint16(encoding.NewParser(bytes.NewReader(buf.Bytes())), &v2)
		require.NoError(t, err)
		assert.Equal(t, n, m)
		assert.Equal(t, v, v2)
	}
}

func TestWriteUint32(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	b := misc.NewBuffer(nil)
	for _, v := range []uint32{
		0, 10, 1073741824,
	} {
		buf.Reset()
		n, err := WriteUint32(buf, b, v)
		require.NoError(t, err)
		var v2 uint32
		m, err := ReadUint32(encoding.NewParser(bytes.NewReader(buf.Bytes())), &v2)
		require.NoError(t, err)
		assert.Equal(t, n, m)
		assert.Equal(t, v, v2)
	}
}

func TestWriteBool(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	b := misc.NewBuffer(nil)
	for _, v := range []bool{
		false, true,
	} {
		buf.Reset()
		n, err := WriteBool(buf, b, v)
		require.NoError(t, err)
		var v2 bool
		m, err := ReadBool(encoding.NewParser(bytes.NewReader(buf.Bytes())), &v2)
		require.NoError(t, err)
		assert.Equal(t, n, m)
		assert.Equal(t, v, v2)
	}
}

func TestWriteFloat64(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	b := misc.NewBuffer(nil)
	for _, v := range []float64{
		0, -10.2, 1073741824,
	} {
		buf.Reset()
		n, err := WriteFloat64(buf, b, v)
		require.NoError(t, err)
		var v2 float64
		m, err := ReadFloat64(encoding.NewParser(bytes.NewReader(buf.Bytes())), &v2)
		require.NoError(t, err)
		assert.Equal(t, n, m)
		assert.Equal(t, v, v2)
	}
}

func TestWriteTime(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	b := misc.NewBuffer(nil)
	for _, v := range []time.Time{
		time.Now().In(time.FixedZone("", 0)),
		time.Now().In(time.FixedZone("", 8)),
		{},
	} {
		buf.Reset()
		n, err := WriteTime(buf, b, v)
		require.NoError(t, err)
		var v2 time.Time
		m, err := ReadTime(encoding.NewParser(bytes.NewReader(buf.Bytes())), &v2)
		require.NoError(t, err)
		assert.Equal(t, n, m)
		assert.True(t, v.Truncate(time.Second).Equal(v2), "%v != %v", v.Truncate(time.Second), v2)
	}
}
