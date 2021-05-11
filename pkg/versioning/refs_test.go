package versioning

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/testutils"
)

func TestSaveRef(t *testing.T) {
	db := kv.NewMockStore(false)
	sum := testutils.SecureRandomBytes(16)
	err := SaveRef(db, "remotes/origin/abc", sum)
	require.NoError(t, err)
	b, err := GetRemoteRef(db, "origin", "abc")
	require.NoError(t, err)
	assert.Equal(t, sum, b)
}

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

func TestRemoteRef(t *testing.T) {
	db := kv.NewMockStore(false)
	remote := "origin"
	name := "abc"
	sum := testutils.SecureRandomBytes(16)
	err := SaveRemoteRef(db, remote, name, sum)
	require.NoError(t, err)
	b, err := GetRemoteRef(db, remote, name)
	require.NoError(t, err)
	assert.Equal(t, sum[:], b)

	err = DeleteRemoteRef(db, remote, name)
	require.NoError(t, err)
	_, err = GetRemoteRef(db, remote, name)
	assert.Error(t, err)
}

func TestListRemoteRefs(t *testing.T) {
	db := kv.NewMockStore(false)
	remote1 := "origin"
	remote2 := "org"
	names := []string{"def", "qwe"}
	sums := [][]byte{
		testutils.SecureRandomBytes(16),
		testutils.SecureRandomBytes(16),
	}

	// test ListRemoteRefs
	for i, name := range names {
		err := SaveRemoteRef(db, remote1, name, sums[i])
		require.NoError(t, err)
	}
	m, err := ListRemoteRefs(db, remote1)
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		names[0]: sums[0],
		names[1]: sums[1],
	}, m)

	// test RenameAllRemoteRefs
	err = RenameAllRemoteRefs(db, remote1, remote2)
	require.NoError(t, err)
	m, err = ListRemoteRefs(db, remote1)
	require.NoError(t, err)
	assert.Len(t, m, 0)
	m, err = ListRemoteRefs(db, remote2)
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		names[0]: sums[0],
		names[1]: sums[1],
	}, m)

	// test DeleteAllRemoteRefs
	err = DeleteAllRemoteRefs(db, remote2)
	require.NoError(t, err)
	m, err = ListRemoteRefs(db, remote2)
	require.NoError(t, err)
	assert.Len(t, m, 0)
}

func TestListAllRefs(t *testing.T) {
	db := kv.NewMockStore(false)
	sum1 := testutils.SecureRandomBytes(16)
	head := "my-branch"
	err := SaveHead(db, head, sum1)
	require.NoError(t, err)
	sum2 := testutils.SecureRandomBytes(16)
	tag := "my-tag"
	err = SaveTag(db, tag, sum2)
	require.NoError(t, err)
	sum3 := testutils.SecureRandomBytes(16)
	remote := "origin"
	name := "main"
	err = SaveRemoteRef(db, remote, name, sum3)
	require.NoError(t, err)

	m, err := ListAllRefs(db)
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"refs/heads/" + head:                            sum1,
		"refs/tags/" + tag:                              sum2,
		fmt.Sprintf("refs/remotes/%s/%s", remote, name): sum3,
	}, m)

	m, err = ListLocalRefs(db)
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"refs/heads/" + head: sum1,
		"refs/tags/" + tag:   sum2,
	}, m)
}
