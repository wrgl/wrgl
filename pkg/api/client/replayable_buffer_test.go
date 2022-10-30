package apiclient

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestReplayableBuffer(t *testing.T) {
	var buf *ReplayableBuffer
	b, err := io.ReadAll(buf)
	require.NoError(t, err)
	assert.Len(t, b, 0)

	buf = NewReplayableBuffer()
	b, err = io.ReadAll(buf)
	require.NoError(t, err)
	assert.Len(t, b, 0)

	buf.Reset()
	off, err := buf.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), off)
	payload := testutils.SecureRandomBytes(100)
	n, err := buf.Write(payload)
	require.NoError(t, err)
	assert.Equal(t, len(payload), n)

	off, err = buf.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), off)
	b, err = io.ReadAll(buf)
	require.NoError(t, err)
	assert.Equal(t, payload, b)

	payload2 := testutils.SecureRandomBytes(200)
	off, err = buf.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(len(payload)), off)
	n, err = buf.Write(payload2)
	require.NoError(t, err)
	assert.Equal(t, len(payload2), n)

	off, err = buf.Seek(int64(-len(payload)-len(payload2)), io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(0), off)
	b, err = io.ReadAll(buf)
	require.NoError(t, err)
	assert.Equal(t, append(payload, payload2...), b)

	buf.Reset()
	payload3 := testutils.SecureRandomBytes(300)
	n, err = buf.Write(payload3)
	require.NoError(t, err)
	assert.Equal(t, len(payload3), n)

	off, err = buf.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), off)
	b, err = io.ReadAll(buf)
	require.NoError(t, err)
	assert.Equal(t, payload3, b)
}
