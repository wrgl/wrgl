package ingest

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/encoding"
)

func TestRowHasher(t *testing.T) {
	hasher := NewRowHasher([]int{0}, 0)
	kh, rh, rc, err := hasher.Sum([]string{"a", "b", "c"})
	require.NoError(t, err)
	assert.Equal(t, "1fb7772791b00e4ec463f657072380c8", hex.EncodeToString(kh))
	assert.Equal(t, "a3c100407f246730f722f7236a953a35", hex.EncodeToString(rh))
	row, err := encoding.DecodeStrings(rc)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, row)

	kh, rh, rc, err = hasher.Sum([]string{"d", "e", "f"})
	require.NoError(t, err)
	assert.Equal(t, "b324c026df2bb2f1d16815545fc4b390", hex.EncodeToString(kh))
	assert.Equal(t, "9183acbd91b4bb6dbc3e74f4ca71085c", hex.EncodeToString(rh))
	row, err = encoding.DecodeStrings(rc)
	require.NoError(t, err)
	assert.Equal(t, []string{"d", "e", "f"}, row)

	hasher = NewRowHasher([]int{1}, 0)
	kh, rh, rc, err = hasher.Sum([]string{"a", "b", "c"})
	require.NoError(t, err)
	assert.Equal(t, "4a3614c34ef827dcb304d48fe2d1eae1", hex.EncodeToString(kh))
	assert.Equal(t, "a3c100407f246730f722f7236a953a35", hex.EncodeToString(rh))
	row, err = encoding.DecodeStrings(rc)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, row)

	hasher = NewRowHasher([]int{}, 0)
	kh, rh, rc, err = hasher.Sum([]string{"a", "b", "c"})
	require.NoError(t, err)
	assert.Equal(t, "a3c100407f246730f722f7236a953a35", hex.EncodeToString(kh))
	assert.Equal(t, "a3c100407f246730f722f7236a953a35", hex.EncodeToString(rh))
	row, err = encoding.DecodeStrings(rc)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, row)
}
