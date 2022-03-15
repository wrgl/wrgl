// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ref_test

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	refmock "github.com/wrgl/wrgl/pkg/ref/mock"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestSaveRef(t *testing.T) {
	s := refmock.NewStore()
	sum := testutils.SecureRandomBytes(16)
	err := ref.SaveRef(s, "heads/beta", sum, "John Doe", "john@doe.com", "branch", "from main")
	require.NoError(t, err)
	b, err := ref.GetHead(s, "beta")
	require.NoError(t, err)
	assert.Equal(t, sum, b)
	b, err = ref.GetRef(s, "heads/beta")
	require.NoError(t, err)
	assert.Equal(t, sum, b)
	refhelpers.AssertLatestReflogEqual(t, s, "heads/beta", &ref.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Action:      "branch",
		Message:     "from main",
	})

	// test RenameRef
	sum1, err := ref.RenameRef(s, "heads/beta", "heads/gamma")
	require.NoError(t, err)
	assert.Equal(t, sum, sum1)
	_, err = ref.GetRef(s, "heads/beta")
	assert.Equal(t, ref.ErrKeyNotFound, err)
	sum1, err = ref.GetRef(s, "heads/gamma")
	require.NoError(t, err)
	assert.Equal(t, sum, sum1)
	_, err = s.LogReader("heads/beta")
	assert.Equal(t, ref.ErrKeyNotFound, err)
	refhelpers.AssertLatestReflogEqual(t, s, "heads/gamma", &ref.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Action:      "branch",
		Message:     "from main",
	})

	// test CopyRef
	sum2, err := ref.CopyRef(s, "heads/gamma", "heads/delta")
	require.NoError(t, err)
	assert.Equal(t, sum, sum2)
	sum2, err = ref.GetRef(s, "heads/gamma")
	require.NoError(t, err)
	assert.Equal(t, sum, sum2)
	sum2, err = ref.GetRef(s, "heads/delta")
	require.NoError(t, err)
	assert.Equal(t, sum, sum2)
	refhelpers.AssertLatestReflogEqual(t, s, "heads/gamma", &ref.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Action:      "branch",
		Message:     "from main",
	})
	refhelpers.AssertLatestReflogEqual(t, s, "heads/delta", &ref.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Action:      "branch",
		Message:     "from main",
	})

	// test SaveFetchRef
	sum3 := testutils.SecureRandomBytes(16)
	require.NoError(t, ref.SaveFetchRef(s, "remotes/origin/abc", sum3, "John Doe", "john@doe.com", "origin", "storing head"))
	refhelpers.AssertLatestReflogEqual(t, s, "remotes/origin/abc", &ref.Reflog{
		NewOID:      sum3,
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Action:      "fetch",
		Message:     "[from origin] storing head",
	})
	b, err = ref.GetRemoteRef(s, "origin", "abc")
	require.NoError(t, err)
	assert.Equal(t, sum3, b)
}

func TestCommitMerge(t *testing.T) {
	s := refmock.NewStore()
	db := objmock.NewStore()
	name := "abc"
	sum1 := testutils.SecureRandomBytes(16)
	sum2 := testutils.SecureRandomBytes(16)
	sum3, commit1 := refhelpers.SaveTestCommit(t, db, [][]byte{sum1, sum2})
	err := ref.CommitMerge(s, name, sum3, commit1)
	require.NoError(t, err)
	b, err := ref.GetHead(s, name)
	require.NoError(t, err)
	assert.Equal(t, sum3, b)
	refhelpers.AssertLatestReflogEqual(t, s, "heads/"+name, &ref.Reflog{
		NewOID:      sum3,
		AuthorName:  commit1.AuthorName,
		AuthorEmail: commit1.AuthorEmail,
		Action:      "merge",
		Message:     fmt.Sprintf("merge %s, %s", hex.EncodeToString(sum1)[:7], hex.EncodeToString(sum2)[:7]),
	})
}

func TestCommitHead(t *testing.T) {
	s := refmock.NewStore()
	db := objmock.NewStore()
	name := "abc"
	sum1, commit1 := refhelpers.SaveTestCommit(t, db, nil)
	err := ref.CommitHead(s, name, sum1, commit1)
	require.NoError(t, err)
	b, err := ref.GetHead(s, name)
	require.NoError(t, err)
	assert.Equal(t, sum1, b)
	refhelpers.AssertLatestReflogEqual(t, s, "heads/"+name, &ref.Reflog{
		NewOID:      sum1,
		AuthorName:  commit1.AuthorName,
		AuthorEmail: commit1.AuthorEmail,
		Action:      "commit",
		Message:     commit1.Message,
	})

	sum2, commit2 := refhelpers.SaveTestCommit(t, db, [][]byte{sum1})
	err = ref.CommitHead(s, name, sum2, commit2)
	require.NoError(t, err)
	b, err = ref.GetHead(s, name)
	require.NoError(t, err)
	assert.Equal(t, sum2, b)
	refhelpers.AssertLatestReflogEqual(t, s, "heads/"+name, &ref.Reflog{
		OldOID:      sum1,
		NewOID:      sum2,
		AuthorName:  commit2.AuthorName,
		AuthorEmail: commit2.AuthorEmail,
		Action:      "commit",
		Message:     commit2.Message,
	})

	name2 := "def"
	sum3, commit3 := refhelpers.SaveTestCommit(t, db, nil)
	err = ref.CommitHead(s, name2, sum3, commit3)
	require.NoError(t, err)
	m, err := ref.ListHeads(s)
	require.NoError(t, err)
	assert.Len(t, m, 2)
	assert.Equal(t, sum2, m[name])
	assert.Equal(t, sum3, m[name2])

	require.NoError(t, ref.DeleteHead(s, name))
	_, err = ref.GetHead(s, name)
	assert.Error(t, err)
	_, err = s.LogReader("heads/" + name)
	assert.Error(t, err)
}

func TestRefTag(t *testing.T) {
	s := refmock.NewStore()
	name := "abc"
	sum1 := testutils.SecureRandomBytes(16)
	err := ref.SaveTag(s, name, sum1)
	require.NoError(t, err)
	b, err := ref.GetTag(s, name)
	require.NoError(t, err)
	assert.Equal(t, sum1, b)

	name2 := "def"
	sum2 := testutils.SecureRandomBytes(16)
	err = ref.SaveTag(s, name2, sum2)
	require.NoError(t, err)
	m, err := ref.ListTags(s)
	require.NoError(t, err)
	assert.Len(t, m, 2)
	assert.Equal(t, sum1[:], m[name])
	assert.Equal(t, sum2[:], m[name2])

	err = ref.DeleteTag(s, "abc")
	require.NoError(t, err)
	_, err = ref.GetTag(s, name)
	assert.Error(t, err)
}

func TestRemoteRef(t *testing.T) {
	s := refmock.NewStore()
	remote := "origin"
	name := "abc"
	sum := testutils.SecureRandomBytes(16)
	err := ref.SaveRemoteRef(s, remote, name, sum, "John Doe", "john@doe.com", "fetch", "from origin")
	require.NoError(t, err)
	b, err := ref.GetRemoteRef(s, remote, name)
	require.NoError(t, err)
	assert.Equal(t, sum, b)
	refhelpers.AssertLatestReflogEqual(t, s, ref.RemoteRef(remote, name), &ref.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Action:      "fetch",
		Message:     "from origin",
	})

	err = ref.DeleteRemoteRef(s, remote, name)
	require.NoError(t, err)
	_, err = ref.GetRemoteRef(s, remote, name)
	assert.Error(t, err)
	_, err = s.LogReader("" + ref.RemoteRef(remote, name))
	assert.Error(t, err)
}

func TestListRemoteRefs(t *testing.T) {
	s := refmock.NewStore()
	remote1 := "origin"
	remote2 := "org"
	names := []string{"def", "qwe"}
	sums := [][]byte{
		testutils.SecureRandomBytes(16),
		testutils.SecureRandomBytes(16),
	}

	// test ListRemoteRefs
	for i, name := range names {
		err := ref.SaveRemoteRef(
			s, remote1, name, sums[i],
			testutils.BrokenRandomAlphaNumericString(5),
			testutils.BrokenRandomAlphaNumericString(10),
			"fetch",
			"from "+remote1,
		)
		require.NoError(t, err)
	}
	m, err := ref.ListRemoteRefs(s, remote1)
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		names[0]: sums[0],
		names[1]: sums[1],
	}, m)

	// test RenameAllRemoteRefs
	err = ref.RenameAllRemoteRefs(s, remote1, remote2)
	require.NoError(t, err)
	m, err = ref.ListRemoteRefs(s, remote1)
	require.NoError(t, err)
	assert.Len(t, m, 0)
	m, err = ref.ListRemoteRefs(s, remote2)
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		names[0]: sums[0],
		names[1]: sums[1],
	}, m)
	sl, err := s.Filter("remotes/" + remote1)
	require.NoError(t, err)
	assert.Len(t, sl, 0)
	sl, err = s.Filter("remotes/" + remote2)
	require.NoError(t, err)
	assert.Len(t, sl, 2)

	// test DeleteAllRemoteRefs
	err = ref.DeleteAllRemoteRefs(s, remote2)
	require.NoError(t, err)
	m, err = ref.ListRemoteRefs(s, remote2)
	require.NoError(t, err)
	assert.Len(t, m, 0)
	sl, err = s.Filter("remotes/" + remote2)
	require.NoError(t, err)
	assert.Len(t, sl, 0)
}

func TestListAllRefs(t *testing.T) {
	s := refmock.NewStore()
	db := objmock.NewStore()
	sum1, commit1 := refhelpers.SaveTestCommit(t, db, nil)
	head := "my-branch"
	err := ref.CommitHead(s, head, sum1, commit1)
	require.NoError(t, err)
	sum2, _ := refhelpers.SaveTestCommit(t, db, nil)
	tag := "my-tag"
	err = ref.SaveTag(s, tag, sum2)
	require.NoError(t, err)
	sum3, _ := refhelpers.SaveTestCommit(t, db, nil)
	remote := "origin"
	name := "main"
	err = ref.SaveRemoteRef(
		s, remote, name, sum3,
		testutils.BrokenRandomAlphaNumericString(5),
		testutils.BrokenRandomAlphaNumericString(10),
		"fetch",
		"from "+remote,
	)
	require.NoError(t, err)

	m, err := ref.ListAllRefs(s)
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"heads/" + head: sum1,
		"tags/" + tag:   sum2,
		fmt.Sprintf("remotes/%s/%s", remote, name): sum3,
	}, m)

	m, err = ref.ListLocalRefs(s)
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"heads/" + head: sum1,
		"tags/" + tag:   sum2,
	}, m)
}

func TestTransactionRef(t *testing.T) {
	s := refmock.NewStore()
	id := uuid.New()
	refs := map[string][]byte{
		"branch-1": testutils.SecureRandomBytes(16),
		"branch-2": testutils.SecureRandomBytes(16),
	}
	for branch, sum := range refs {
		require.NoError(t, ref.SaveTransactionRef(s, id, branch, sum))
	}

	m, err := ref.ListTransactionRefs(s, id)
	require.NoError(t, err)
	assert.Equal(t, refs, m)

	require.NoError(t, ref.DeleteTransactionRefs(s, id))

	m, err = ref.ListTransactionRefs(s, id)
	require.NoError(t, err)
	assert.Len(t, m, 0)
}
