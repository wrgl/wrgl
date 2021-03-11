package versioning

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wrgl/core/pkg/kv"
)

func TestTableEncode(t *testing.T) {
	s := kv.NewMockStore(false)
	ta1 := &Table{
		Columns:     []string{"a", "b", "c"},
		PrimaryKeys: []int{0},
		Rows: []KeyHash{
			{K: "abc", V: []byte("123")},
		},
	}
	orgID := "ghkjh"
	repo := "repo"
	hash, err := ta1.Save(s, orgID, repo, 0)
	require.NoError(t, err)
	var ta2 *Table
	assert.Equal(t, "016ddd97d8967d07ec40f793d0967303", hash)
	ta2, err = GetTable(s, orgID, repo, hash)
	require.NoError(t, err)
	assert.Equal(t, ta1, ta2)
	assert.True(t, TableExist(s, orgID, repo, hash))

	ta3 := &Table{
		Columns:     []string{"a", "b", "c"},
		PrimaryKeys: []int{0},
		Rows: []KeyHash{
			{K: "abc", V: []byte("456")},
		},
	}
	hash2, err := ta3.Save(s, orgID, repo, 0)
	require.NoError(t, err)

	sl, err := GetAllTableHashes(s, orgID, repo)
	require.NoError(t, err)
	assert.Len(t, sl, 2)

	err = DeleteTable(s, orgID, repo, hash)
	require.NoError(t, err)
	_, err = GetTable(s, orgID, repo, hash)
	assert.Equal(t, kv.KeyNotFoundError, err)

	err = DeleteAllTables(s, orgID, repo)
	require.NoError(t, err)
	_, err = GetTable(s, orgID, repo, hash2)
	assert.Equal(t, kv.KeyNotFoundError, err)

}
