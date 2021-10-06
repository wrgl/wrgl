package index

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestHashSet(t *testing.T) {
	f, err := ioutil.TempFile("", "test_hash_set")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := NewHashSet(f, 0)
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
	require.NoError(t, f.Close())

	// add more values
	f, err = os.OpenFile(f.Name(), os.O_RDWR, 0666)
	require.NoError(t, err)
	s, err = NewHashSet(f, 1)
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
	require.NoError(t, f.Close())

	// add a lot more values
	f, err = os.OpenFile(f.Name(), os.O_RDWR, 0666)
	require.NoError(t, err)
	s, err = NewHashSet(f, 0)
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
	require.NoError(t, f.Close())
}
