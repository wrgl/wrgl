// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package versioning

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
)

func TestSaveRef(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	sum := testutils.SecureRandomBytes(16)
	err := SaveRef(db, fs, "remotes/origin/abc", sum, "John Doe", "john@doe.com", "fetch", "from origin")
	require.NoError(t, err)
	b, err := GetRemoteRef(db, "origin", "abc")
	require.NoError(t, err)
	assert.Equal(t, sum, b)
	b, err = GetRef(db, "remotes/origin/abc")
	require.NoError(t, err)
	assert.Equal(t, sum, b)
	AssertLatestReflogEqual(t, fs, "remotes/origin/abc", &objects.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Action:      "fetch",
		Message:     "from origin",
	})

	// test RenameRef
	sum1, err := RenameRef(db, fs, "remotes/origin/abc", "remotes/origin2/abc")
	require.NoError(t, err)
	assert.Equal(t, sum, sum1)
	_, err = GetRef(db, "remotes/origin/abc")
	assert.Error(t, err)
	sum1, err = GetRef(db, "remotes/origin2/abc")
	require.NoError(t, err)
	assert.Equal(t, sum, sum1)
	_, err = fs.Reader([]byte("logs/refs/remotes/origin/abc"))
	assert.Error(t, err)
	AssertLatestReflogEqual(t, fs, "remotes/origin2/abc", &objects.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Action:      "fetch",
		Message:     "from origin",
	})

	// test CopyRef
	sum2, err := CopyRef(db, fs, "remotes/origin2/abc", "remotes/origin2/def")
	require.NoError(t, err)
	assert.Equal(t, sum, sum2)
	sum2, err = GetRef(db, "remotes/origin2/abc")
	require.NoError(t, err)
	assert.Equal(t, sum, sum2)
	sum2, err = GetRef(db, "remotes/origin2/def")
	require.NoError(t, err)
	assert.Equal(t, sum, sum2)
	AssertLatestReflogEqual(t, fs, "remotes/origin2/abc", &objects.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Action:      "fetch",
		Message:     "from origin",
	})
	AssertLatestReflogEqual(t, fs, "remotes/origin2/def", &objects.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Action:      "fetch",
		Message:     "from origin",
	})
}

func TestCommitMerge(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	name := "abc"
	sum1, _ := SaveTestCommit(t, db, nil)
	sum2, _ := SaveTestCommit(t, db, nil)
	sum3, commit1 := SaveTestCommit(t, db, [][]byte{sum1, sum2})
	err := CommitMerge(db, fs, name, sum3, commit1)
	require.NoError(t, err)
	b, err := GetHead(db, name)
	require.NoError(t, err)
	assert.Equal(t, sum3, b)
	AssertLatestReflogEqual(t, fs, "heads/"+name, &objects.Reflog{
		NewOID:      sum3,
		AuthorName:  commit1.AuthorName,
		AuthorEmail: commit1.AuthorEmail,
		Action:      "merge",
		Message:     fmt.Sprintf("merge %s, %s", hex.EncodeToString(sum1)[:7], hex.EncodeToString(sum2)[:7]),
	})
}

func TestCommitHead(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	name := "abc"
	sum1, commit1 := SaveTestCommit(t, db, nil)
	err := CommitHead(db, fs, name, sum1, commit1)
	require.NoError(t, err)
	b, err := GetHead(db, name)
	require.NoError(t, err)
	assert.Equal(t, sum1, b)
	AssertLatestReflogEqual(t, fs, "heads/"+name, &objects.Reflog{
		NewOID:      sum1,
		AuthorName:  commit1.AuthorName,
		AuthorEmail: commit1.AuthorEmail,
		Action:      "commit",
		Message:     commit1.Message,
	})

	sum2, commit2 := SaveTestCommit(t, db, [][]byte{sum1})
	err = CommitHead(db, fs, name, sum2, commit2)
	require.NoError(t, err)
	b, err = GetHead(db, name)
	require.NoError(t, err)
	assert.Equal(t, sum2, b)
	AssertLatestReflogEqual(t, fs, "heads/"+name, &objects.Reflog{
		OldOID:      sum1,
		NewOID:      sum2,
		AuthorName:  commit2.AuthorName,
		AuthorEmail: commit2.AuthorEmail,
		Action:      "commit",
		Message:     commit2.Message,
	})

	name2 := "def"
	sum3, commit3 := SaveTestCommit(t, db, nil)
	err = CommitHead(db, fs, name2, sum3, commit3)
	require.NoError(t, err)
	m, err := ListHeads(db)
	require.NoError(t, err)
	assert.Len(t, m, 2)
	assert.Equal(t, sum2, m[name])
	assert.Equal(t, sum3, m[name2])

	require.NoError(t, DeleteHead(db, fs, name))
	_, err = GetHead(db, name)
	assert.Error(t, err)
	_, err = fs.Reader([]byte("logs/refs/heads/" + name))
	assert.Error(t, err)
}

func TestRefTag(t *testing.T) {
	db := kv.NewMockStore(false)
	name := "abc"
	sum1 := testutils.SecureRandomBytes(16)
	err := SaveTag(db, name, sum1)
	require.NoError(t, err)
	b, err := GetTag(db, name)
	require.NoError(t, err)
	assert.Equal(t, sum1, b)

	name2 := "def"
	sum2 := testutils.SecureRandomBytes(16)
	err = SaveTag(db, name2, sum2)
	require.NoError(t, err)
	m, err := ListTags(db)
	require.NoError(t, err)
	assert.Len(t, m, 2)
	assert.Equal(t, sum1[:], m[name])
	assert.Equal(t, sum2[:], m[name2])

	err = DeleteTag(db, "abc")
	require.NoError(t, err)
	_, err = GetTag(db, name)
	assert.Error(t, err)
}

func TestRemoteRef(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	remote := "origin"
	name := "abc"
	sum := testutils.SecureRandomBytes(16)
	err := SaveRemoteRef(db, fs, remote, name, sum, "John Doe", "john@doe.com", "fetch", "from origin")
	require.NoError(t, err)
	b, err := GetRemoteRef(db, remote, name)
	require.NoError(t, err)
	assert.Equal(t, sum, b)
	AssertLatestReflogEqual(t, fs, remoteRef(remote, name), &objects.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Action:      "fetch",
		Message:     "from origin",
	})

	err = DeleteRemoteRef(db, fs, remote, name)
	require.NoError(t, err)
	_, err = GetRemoteRef(db, remote, name)
	assert.Error(t, err)
	_, err = fs.Reader([]byte("logs/refs/" + remoteRef(remote, name)))
	assert.Error(t, err)
}

func TestListRemoteRefs(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	remote1 := "origin"
	remote2 := "org"
	names := []string{"def", "qwe"}
	sums := [][]byte{
		testutils.SecureRandomBytes(16),
		testutils.SecureRandomBytes(16),
	}

	// test ListRemoteRefs
	for i, name := range names {
		err := SaveRemoteRef(
			db, fs, remote1, name, sums[i],
			testutils.BrokenRandomAlphaNumericString(5),
			testutils.BrokenRandomAlphaNumericString(10),
			"fetch",
			"from "+remote1,
		)
		require.NoError(t, err)
	}
	m, err := ListRemoteRefs(db, remote1)
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		names[0]: sums[0],
		names[1]: sums[1],
	}, m)

	// test RenameAllRemoteRefs
	err = RenameAllRemoteRefs(db, fs, remote1, remote2)
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
	sl, err := fs.Filter([]byte("logs/refs/remotes/" + remote1))
	require.NoError(t, err)
	assert.Len(t, sl, 0)
	sl, err = fs.Filter([]byte("logs/refs/remotes/" + remote2))
	require.NoError(t, err)
	assert.Len(t, sl, 2)

	// test DeleteAllRemoteRefs
	err = DeleteAllRemoteRefs(db, fs, remote2)
	require.NoError(t, err)
	m, err = ListRemoteRefs(db, remote2)
	require.NoError(t, err)
	assert.Len(t, m, 0)
	sl, err = fs.Filter([]byte("logs/refs/remotes/" + remote2))
	require.NoError(t, err)
	assert.Len(t, sl, 0)
}

func TestListAllRefs(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	sum1, commit1 := SaveTestCommit(t, db, nil)
	head := "my-branch"
	err := CommitHead(db, fs, head, sum1, commit1)
	require.NoError(t, err)
	sum2, _ := SaveTestCommit(t, db, nil)
	tag := "my-tag"
	err = SaveTag(db, tag, sum2)
	require.NoError(t, err)
	sum3, _ := SaveTestCommit(t, db, nil)
	remote := "origin"
	name := "main"
	err = SaveRemoteRef(
		db, fs, remote, name, sum3,
		testutils.BrokenRandomAlphaNumericString(5),
		testutils.BrokenRandomAlphaNumericString(10),
		"fetch",
		"from "+remote,
	)
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
