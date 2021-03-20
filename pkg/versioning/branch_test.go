package versioning

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
)

func TestRepo(t *testing.T) {
	db := kv.NewMockStore(false)
	r := &Branch{
		CommitHash: "abcd1234",
	}
	name := "abc"
	err := r.Save(db, name)
	require.NoError(t, err)
	r2, err := GetBranch(db, name)
	require.NoError(t, err)
	assert.Equal(t, r.CommitHash, r2.CommitHash)
}
