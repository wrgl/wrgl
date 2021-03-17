package versioning

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
)

func TestRepo(t *testing.T) {
	db := kv.NewMockStore(false)
	r := &Repo{
		CommitHash: "abcd1234",
	}
	err := r.Save(db)
	require.NoError(t, err)
	r2, err := GetRepo(db)
	require.NoError(t, err)
	assert.Equal(t, r.CommitHash, r2.CommitHash)
}
