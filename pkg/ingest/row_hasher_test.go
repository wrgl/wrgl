package ingest

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/objects"
)

func TestRowHasher(t *testing.T) {
	hasher := NewRowHasher([]uint32{0}, 0)
	kh, rh, rc, err := hasher.Sum([]string{"a", "b", "c"})
	require.NoError(t, err)
	assert.Equal(t, "e5a364171a46ea44556d3cd97e99f3a8", hex.EncodeToString(kh))
	assert.Equal(t, "eef20ff2a729c390fcac9f0fd26ffdda", hex.EncodeToString(rh))
	dec := objects.NewStrListDecoder(true)
	row := dec.Decode(rc)
	assert.Equal(t, []string{"a", "b", "c"}, row)

	kh, rh, rc, err = hasher.Sum([]string{"d", "e", "f"})
	require.NoError(t, err)
	assert.Equal(t, "44f2d02e49bab72155e8319b62c839cc", hex.EncodeToString(kh))
	assert.Equal(t, "98afafdab4b3d3b689ccbed342fbad61", hex.EncodeToString(rh))
	row = dec.Decode(rc)
	assert.Equal(t, []string{"d", "e", "f"}, row)

	hasher = NewRowHasher([]uint32{1}, 0)
	kh, rh, rc, err = hasher.Sum([]string{"a", "b", "c"})
	require.NoError(t, err)
	assert.Equal(t, "b8e208a0393ea214384d17f17a440095", hex.EncodeToString(kh))
	assert.Equal(t, "eef20ff2a729c390fcac9f0fd26ffdda", hex.EncodeToString(rh))
	row = dec.Decode(rc)
	assert.Equal(t, []string{"a", "b", "c"}, row)

	hasher = NewRowHasher([]uint32{}, 0)
	kh, rh, rc, err = hasher.Sum([]string{"a", "b", "c"})
	require.NoError(t, err)
	assert.Equal(t, "eef20ff2a729c390fcac9f0fd26ffdda", hex.EncodeToString(kh))
	assert.Equal(t, "eef20ff2a729c390fcac9f0fd26ffdda", hex.EncodeToString(rh))
	row = dec.Decode(rc)
	assert.Equal(t, []string{"a", "b", "c"}, row)
}
