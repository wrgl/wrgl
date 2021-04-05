package versioning

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
)

func TestBranch(t *testing.T) {
	db := kv.NewMockStore(false)
	b := &objects.Branch{
		CommitSum: []byte("abcd1234"),
	}
	name := "abc"
	err := SaveBranch(db, name, b)
	require.NoError(t, err)
	b2, err := GetBranch(db, name)
	require.NoError(t, err)
	assert.Equal(t, b.CommitSum, b2.CommitSum)

	b3 := &objects.Branch{CommitSum: []byte("qwer2345")}
	name2 := "def"
	err = SaveBranch(db, name2, b3)
	require.NoError(t, err)
	m, err := ListBranch(db)
	require.NoError(t, err)
	assert.Len(t, m, 2)
	assert.Equal(t, b.CommitSum, m[name].CommitSum)
	assert.Equal(t, b3.CommitSum, m[name2].CommitSum)

	err = DeleteBranch(db, "abc")
	require.NoError(t, err)
	_, err = GetBranch(db, name)
	assert.Error(t, err)
}
