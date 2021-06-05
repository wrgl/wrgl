package index

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/testutils"
)

type nopCloser struct {
	r io.ReadWriteSeeker
}

func (n *nopCloser) Read(b []byte) (int, error) {
	return n.r.Read(b)
}

func (n *nopCloser) Write(b []byte) (int, error) {
	return n.r.Write(b)
}

func (n *nopCloser) Seek(off int64, whence int) (int64, error) {
	return n.r.Seek(off, whence)
}

func (n *nopCloser) Close() error {
	return nil
}

func NopCloser(r io.ReadWriteSeeker) ReadWriteSeekCloser {
	return &nopCloser{r: r}
}

func TestHashSet(t *testing.T) {
	buf := misc.NewBuffer(nil)
	s, err := NewHashSet(NopCloser(buf), 0)
	require.NoError(t, err)

	sl := [][]byte{}
	for i := 0; i < 3; i++ {
		sl = append(sl, testutils.SecureRandomBytes(16))
		require.NoError(t, s.Add(sl[i]))
	}
	require.NoError(t, s.Flush())
	for _, h := range sl {
		ok, err := s.Has(h)
		require.NoError(t, err)
		assert.True(t, ok)
	}

	// add more values
	s, err = NewHashSet(NopCloser(buf), 1)
	require.NoError(t, err)
	assert.Equal(t, 3, s.Len())
	for i := 0; i < 3; i++ {
		sl = append(sl, testutils.SecureRandomBytes(16))
	}
	for i := 0; i < 3; i++ {
		ok, err := s.Has(sl[i])
		require.NoError(t, err)
		assert.True(t, ok)
	}
	for i := 3; i < 6; i++ {
		ok, err := s.Has(sl[i])
		require.NoError(t, err)
		assert.False(t, ok)
	}
	for _, h := range sl {
		require.NoError(t, s.Add(h))
	}
	assert.Equal(t, 6, s.Len())
	for _, h := range sl {
		ok, err := s.Has(h)
		require.NoError(t, err)
		assert.True(t, ok, "hash not found %x", h)
	}

	// add a lot more values
	s, err = NewHashSet(NopCloser(buf), 0)
	require.NoError(t, err)
	assert.Equal(t, 6, s.Len())
	for i := 0; i < 2048; i++ {
		b := testutils.SecureRandomBytes(16)
		sl = append(sl, b)
		ok, err := s.Has(b)
		require.NoError(t, err)
		assert.False(t, ok)
		require.NoError(t, s.Add(b))
	}
	require.NoError(t, s.Flush())
	for _, h := range sl {
		ok, err := s.Has(h)
		require.NoError(t, err)
		assert.True(t, ok, "hash not found %x", h)
	}
}
