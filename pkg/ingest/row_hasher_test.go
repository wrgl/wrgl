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
	assert.Equal(t, "e49dd6a032e1b3c22c4c8239fb68d26a", hex.EncodeToString(kh))
	assert.Equal(t, "48c31f77db575711d74a94602533b815", hex.EncodeToString(rh))
	dec := objects.NewStrListDecoder(true)
	row := dec.Decode(rc)
	assert.Equal(t, []string{"a", "b", "c"}, row)

	kh, rh, rc, err = hasher.Sum([]string{"d", "e", "f"})
	require.NoError(t, err)
	assert.Equal(t, "d74b96541b47a1e1b3f157227134c244", hex.EncodeToString(kh))
	assert.Equal(t, "704751f79f24197a348fa7f942743914", hex.EncodeToString(rh))
	row = dec.Decode(rc)
	assert.Equal(t, []string{"d", "e", "f"}, row)

	hasher = NewRowHasher([]uint32{1}, 0)
	kh, rh, rc, err = hasher.Sum([]string{"a", "b", "c"})
	require.NoError(t, err)
	assert.Equal(t, "2c7cfd86e4bc3d8dc9e91ce4e6ffccdb", hex.EncodeToString(kh))
	assert.Equal(t, "48c31f77db575711d74a94602533b815", hex.EncodeToString(rh))
	row = dec.Decode(rc)
	assert.Equal(t, []string{"a", "b", "c"}, row)

	hasher = NewRowHasher([]uint32{}, 0)
	kh, rh, rc, err = hasher.Sum([]string{"a", "b", "c"})
	require.NoError(t, err)
	assert.Equal(t, "48c31f77db575711d74a94602533b815", hex.EncodeToString(kh))
	assert.Equal(t, "48c31f77db575711d74a94602533b815", hex.EncodeToString(rh))
	row = dec.Decode(rc)
	assert.Equal(t, []string{"a", "b", "c"}, row)
}
