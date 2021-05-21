// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/pack"
	packtest "github.com/wrgl/core/pkg/pack/test"
	"github.com/wrgl/core/pkg/versioning"
)

func TestFetchCmd(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	dbs := kv.NewMockStore(false)
	fss := kv.NewMockStore(false)
	sum1, c1 := factory.CommitRandom(t, dbs, fss, nil)
	sum2, c2 := factory.CommitRandom(t, dbs, fss, [][]byte{sum1})
	sum3, c3 := factory.CommitRandom(t, dbs, fss, nil)
	sum4, c4 := factory.CommitRandom(t, dbs, fss, [][]byte{sum3})
	require.NoError(t, versioning.CommitHead(dbs, fss, "main", sum2, c2))
	require.NoError(t, versioning.CommitHead(dbs, fss, "tickets", sum4, c4))
	require.NoError(t, versioning.SaveTag(dbs, "2020", sum1))
	packtest.RegisterHandler(http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(dbs))
	packtest.RegisterHandler(http.MethodPost, "/upload-pack/", pack.NewUploadPackHandler(dbs, fss))

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	packtest.CopyCommitsToNewStore(t, dbs, db, fss, fs, [][]byte{sum1, sum3})
	require.NoError(t, versioning.CommitHead(db, fs, "main", sum1, c1))
	require.NoError(t, versioning.CommitHead(db, fs, "tickets", sum3, c3))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", packtest.TestOrigin})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + packtest.TestOrigin,
		" * [new branch]        main        -> origin/main",
		" * [new branch]        tickets     -> origin/tickets",
		" * [new tag]           2020        -> 2020",
		"",
	}, "\n"))
	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	sum, err := versioning.GetRemoteRef(db, "origin", "main")
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	sum, err = versioning.GetRemoteRef(db, "origin", "tickets")
	require.NoError(t, err)
	assert.Equal(t, sum4, sum)
	sum, err = versioning.GetTag(db, "2020")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	packtest.AssertCommitsPersisted(t, db, fs, [][]byte{sum2, sum4})
	versioning.AssertLatestReflogEqual(t, fs, "remotes/origin/main", &objects.Reflog{
		NewOID:      sum2,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "storing head",
	})
	versioning.AssertLatestReflogEqual(t, fs, "remotes/origin/tickets", &objects.Reflog{
		NewOID:      sum4,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "storing head",
	})
}

func TestFetchCmdAllRepos(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()

	db1 := kv.NewMockStore(false)
	fs1 := kv.NewMockStore(false)
	sum1, c1 := factory.CommitRandom(t, db1, fs1, nil)
	require.NoError(t, versioning.CommitHead(db1, fs1, "main", sum1, c1))
	url1 := "https://origin.remote"
	packtest.RegisterHandlerWithOrigin(url1, http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(db1))
	packtest.RegisterHandlerWithOrigin(url1, http.MethodPost, "/upload-pack/", pack.NewUploadPackHandler(db1, fs1))

	db2 := kv.NewMockStore(false)
	fs2 := kv.NewMockStore(false)
	sum2, c2 := factory.CommitRandom(t, db2, fs2, nil)
	require.NoError(t, versioning.CommitHead(db2, fs2, "main", sum2, c2))
	url2 := "https://acme.remote"
	packtest.RegisterHandlerWithOrigin(url2, http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(db2))
	packtest.RegisterHandlerWithOrigin(url2, http.MethodPost, "/upload-pack/", pack.NewUploadPackHandler(db2, fs2))

	db3 := kv.NewMockStore(false)
	fs3 := kv.NewMockStore(false)
	sum3, c3 := factory.CommitRandom(t, db3, fs3, nil)
	require.NoError(t, versioning.CommitHead(db3, fs3, "main", sum3, c3))
	url3 := "https://home.remote"
	packtest.RegisterHandlerWithOrigin(url3, http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(db3))
	packtest.RegisterHandlerWithOrigin(url3, http.MethodPost, "/upload-pack/", pack.NewUploadPackHandler(db3, fs3))

	rd, cleanUp := createRepoDir(t)
	fs := rd.OpenFileStore()
	defer cleanUp()

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", url1})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "acme", url2})
	require.NoError(t, cmd.Execute())
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "home", url3})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "acme"})
	require.NoError(t, cmd.Execute())
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	sum, err := versioning.GetRemoteRef(db, "acme", "main")
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	_, err = versioning.GetRemoteRef(db, "origin", "main")
	assert.Equal(t, kv.KeyNotFoundError, err)
	_, err = versioning.GetRemoteRef(db, "home", "main")
	assert.Equal(t, kv.KeyNotFoundError, err)
	packtest.AssertCommitsPersisted(t, db, fs, [][]byte{sum2})
	require.NoError(t, db.Close())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "--all"})
	require.NoError(t, cmd.Execute())
	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	sum, err = versioning.GetRemoteRef(db, "origin", "main")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	sum, err = versioning.GetRemoteRef(db, "home", "main")
	require.NoError(t, err)
	assert.Equal(t, sum3, sum)
	packtest.AssertCommitsPersisted(t, db, fs, [][]byte{sum1, sum3})
	require.NoError(t, db.Close())
}

func TestFetchCmdCustomRefSpec(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()

	db1 := kv.NewMockStore(false)
	fs1 := kv.NewMockStore(false)
	sum1, _ := factory.CommitRandom(t, db1, fs1, nil)
	require.NoError(t, versioning.SaveRef(db1, fs1, "custom/abc", sum1, "test", "test@domain.com", "test", "test fetch custom"))
	packtest.RegisterHandler(http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(db1))
	packtest.RegisterHandler(http.MethodPost, "/upload-pack/", pack.NewUploadPackHandler(db1, fs1))

	rd, cleanUp := createRepoDir(t)
	fs := rd.OpenFileStore()
	defer cleanUp()

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", packtest.TestOrigin})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "origin", "refs/custom/abc:refs/custom/abc"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + packtest.TestOrigin,
		" * [new ref]           refs/custom/abc -> refs/custom/abc",
		"",
	}, "\n"))
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	sum, err := versioning.GetRef(db, "custom/abc")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	packtest.AssertCommitsPersisted(t, db, fs, [][]byte{sum1})
	versioning.AssertLatestReflogEqual(t, fs, "custom/abc", &objects.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "storing ref",
	})
}

func TestFetchCmdTag(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()

	db1 := kv.NewMockStore(false)
	fs1 := kv.NewMockStore(false)
	sum1, _ := factory.CommitRandom(t, db1, fs1, nil)
	require.NoError(t, versioning.SaveTag(db1, "2020-dec", sum1))
	sum2, _ := factory.CommitRandom(t, db1, fs1, nil)
	require.NoError(t, versioning.SaveTag(db1, "2021-dec", sum2))
	packtest.RegisterHandler(http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(db1))
	packtest.RegisterHandler(http.MethodPost, "/upload-pack/", pack.NewUploadPackHandler(db1, fs1))

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	sum3, _ := factory.CommitRandom(t, db, fs, nil)
	require.NoError(t, versioning.SaveTag(db, "2020-dec", sum3))
	sum4, _ := factory.CommitRandom(t, db, fs, nil)
	require.NoError(t, versioning.SaveTag(db, "2021-dec", sum4))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", packtest.TestOrigin})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "origin", "refs/tags/202*:refs/tags/202*"})
	assertCmdFailed(t, cmd, strings.Join([]string{
		"From " + packtest.TestOrigin,
		" ! [rejected]          2020-dec    -> 2020-dec (would clobber existing tag)",
		" ! [rejected]          2021-dec    -> 2021-dec (would clobber existing tag)",
		"",
	}, "\n"), fmt.Errorf("failed to fetch some refs from "+packtest.TestOrigin))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "origin", "+refs/tags/2020*:refs/tags/2020*"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + packtest.TestOrigin,
		" t [tag update]        2020-dec    -> 2020-dec",
		"",
	}, "\n"))
	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	sum, err := versioning.GetTag(db, "2020-dec")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	packtest.AssertCommitsPersisted(t, db, fs, [][]byte{sum1})
	versioning.AssertLatestReflogEqual(t, fs, "tags/2020-dec", &objects.Reflog{
		OldOID:      sum3,
		NewOID:      sum1,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "updating tag",
	})
	require.NoError(t, db.Close())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "origin", "refs/tags/2021*:refs/tags/2021*", "--force"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + packtest.TestOrigin,
		" t [tag update]        2021-dec    -> 2021-dec",
		"",
	}, "\n"))
	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	sum, err = versioning.GetTag(db, "2021-dec")
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	packtest.AssertCommitsPersisted(t, db, fs, [][]byte{sum2})
	versioning.AssertLatestReflogEqual(t, fs, "tags/2021-dec", &objects.Reflog{
		OldOID:      sum4,
		NewOID:      sum2,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "updating tag",
	})
	require.NoError(t, db.Close())
}

func TestFetchCmdForceUpdate(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()

	db1 := kv.NewMockStore(false)
	fs1 := kv.NewMockStore(false)
	sum1, c1 := factory.CommitRandom(t, db1, fs1, nil)
	require.NoError(t, versioning.CommitHead(db1, fs1, "abc", sum1, c1))
	packtest.RegisterHandler(http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(db1))
	packtest.RegisterHandler(http.MethodPost, "/upload-pack/", pack.NewUploadPackHandler(db1, fs1))

	rd, cleanUp := createRepoDir(t)
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	defer cleanUp()
	fs := rd.OpenFileStore()
	sum2, c2 := factory.CommitRandom(t, db, fs, nil)
	require.NoError(t, versioning.CommitHead(db, fs, "abc", sum2, c2))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", packtest.TestOrigin})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "origin", "refs/heads/abc:refs/heads/abc"})
	assertCmdFailed(t, cmd, strings.Join([]string{
		"From " + packtest.TestOrigin,
		" ! [rejected]          abc         -> abc (non-fast-forward)",
		"",
	}, "\n"), fmt.Errorf("failed to fetch some refs from "+packtest.TestOrigin))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "origin", "+refs/heads/abc:refs/heads/abc"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + packtest.TestOrigin,
		fmt.Sprintf(" + %s..%s    abc         -> abc (forced update)", hex.EncodeToString(sum2)[:7], hex.EncodeToString(sum1)[:7]),
		"",
	}, "\n"))

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	sum, err := versioning.GetHead(db, "abc")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	packtest.AssertCommitsPersisted(t, db, fs, [][]byte{sum1})
	versioning.AssertLatestReflogEqual(t, fs, "heads/abc", &objects.Reflog{
		OldOID:      sum2,
		NewOID:      sum1,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "forced-update",
	})
}
