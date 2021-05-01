package versioning

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/testutils"
)

func TestRefHead(t *testing.T) {
	db := kv.NewMockStore(false)
	name := "abc"
	sum := testutils.SecureRandomBytes(16)
	err := SaveHead(db, name, sum)
	require.NoError(t, err)
	b, err := GetHead(db, name)
	require.NoError(t, err)
	assert.Equal(t, sum[:], b)

	name2 := "def"
	sum2 := testutils.SecureRandomBytes(16)
	err = SaveHead(db, name2, sum2)
	require.NoError(t, err)
	m, err := ListHeads(db)
	require.NoError(t, err)
	assert.Len(t, m, 2)
	assert.Equal(t, sum[:], m[name])
	assert.Equal(t, sum2[:], m[name2])

	err = DeleteHead(db, "abc")
	require.NoError(t, err)
	_, err = GetHead(db, name)
	assert.Error(t, err)
}

func TestRefTag(t *testing.T) {
	db := kv.NewMockStore(false)
	name := "abc"
	sum := testutils.SecureRandomBytes(16)
	err := SaveTag(db, name, sum)
	require.NoError(t, err)
	b, err := GetTag(db, name)
	require.NoError(t, err)
	assert.Equal(t, sum[:], b)

	name2 := "def"
	sum2 := testutils.SecureRandomBytes(16)
	err = SaveTag(db, name2, sum2)
	require.NoError(t, err)
	m, err := ListTags(db)
	require.NoError(t, err)
	assert.Len(t, m, 2)
	assert.Equal(t, sum[:], m[name])
	assert.Equal(t, sum2[:], m[name2])

	err = DeleteTag(db, "abc")
	require.NoError(t, err)
	_, err = GetTag(db, name)
	assert.Error(t, err)
}
