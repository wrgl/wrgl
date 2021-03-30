package versioning

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
)

func TestBranch(t *testing.T) {
	db := kv.NewMockStore(false)
	b := &Branch{
		CommitHash: "abcd1234",
	}
	name := "abc"
	err := b.Save(db, name)
	require.NoError(t, err)
	b2, err := GetBranch(db, name)
	require.NoError(t, err)
	assert.Equal(t, b.CommitHash, b2.CommitHash)

	b3 := &Branch{CommitHash: "qwer2345"}
	name2 := "def"
	err = b3.Save(db, name2)
	require.NoError(t, err)
	m, err := ListBranch(db)
	require.NoError(t, err)
	assert.Equal(t, map[string]*Branch{
		name:  b,
		name2: b3,
	}, m)

	err = DeleteBranch(db, "abc")
	require.NoError(t, err)
	_, err = GetBranch(db, name)
	assert.Error(t, err)
}
