// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/api"
	apitest "github.com/wrgl/core/pkg/api/test"
	"github.com/wrgl/core/pkg/factory"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/ref"
	refhelpers "github.com/wrgl/core/pkg/ref/helpers"
	refmock "github.com/wrgl/core/pkg/ref/mock"
)

func TestFetchCmd(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	dbs := objmock.NewStore()
	rss := refmock.NewStore()
	sum1, c1 := factory.CommitRandom(t, dbs, nil)
	sum2, c2 := factory.CommitRandom(t, dbs, [][]byte{sum1})
	sum3, c3 := factory.CommitRandom(t, dbs, nil)
	sum4, c4 := factory.CommitRandom(t, dbs, [][]byte{sum3})
	require.NoError(t, ref.CommitHead(rss, "main", sum2, c2))
	require.NoError(t, ref.CommitHead(rss, "tickets", sum4, c4))
	require.NoError(t, ref.SaveTag(rss, "2020", sum1))
	apitest.RegisterHandler(http.MethodGet, "/info/refs/", api.NewInfoRefsHandler(rss))
	apitest.RegisterHandler(http.MethodPost, "/upload-pack/", api.NewUploadPackHandler(dbs, rss, 0))

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	apitest.CopyCommitsToNewStore(t, dbs, db, [][]byte{sum1, sum3})
	require.NoError(t, ref.CommitHead(rs, "main", sum1, c1))
	require.NoError(t, ref.CommitHead(rs, "tickets", sum3, c3))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", apitest.TestOrigin})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + apitest.TestOrigin,
		" * [new branch]      main        -> origin/main",
		" * [new branch]      tickets     -> origin/tickets",
		" * [new tag]         2020        -> 2020",
		"",
	}, "\n"))
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	sum, err := ref.GetRemoteRef(rs, "origin", "main")
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	sum, err = ref.GetRemoteRef(rs, "origin", "tickets")
	require.NoError(t, err)
	assert.Equal(t, sum4, sum)
	sum, err = ref.GetTag(rs, "2020")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	apitest.AssertCommitsPersisted(t, db, [][]byte{sum2, sum4})
	refhelpers.AssertLatestReflogEqual(t, rs, "remotes/origin/main", &ref.Reflog{
		NewOID:      sum2,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "storing head",
	})
	refhelpers.AssertLatestReflogEqual(t, rs, "remotes/origin/tickets", &ref.Reflog{
		NewOID:      sum4,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "storing head",
	})
	require.NoError(t, db.Close())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch"})
	assertCmdOutput(t, cmd, "")
}

func assertCommandNoErr(t *testing.T, cmd *cobra.Command) {
	t.Helper()
	cmd.SetOut(io.Discard)
	require.NoError(t, cmd.Execute())
}

func TestFetchCmdAllRepos(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()

	db1 := objmock.NewStore()
	rs1 := refmock.NewStore()
	sum1, c1 := factory.CommitRandom(t, db1, nil)
	require.NoError(t, ref.CommitHead(rs1, "main", sum1, c1))
	url1 := "https://origin.remote"
	apitest.RegisterHandlerWithOrigin(url1, http.MethodGet, "/info/refs/", api.NewInfoRefsHandler(rs1))
	apitest.RegisterHandlerWithOrigin(url1, http.MethodPost, "/upload-pack/", api.NewUploadPackHandler(db1, rs1, 0))

	db2 := objmock.NewStore()
	rs2 := refmock.NewStore()
	sum2, c2 := factory.CommitRandom(t, db2, nil)
	require.NoError(t, ref.CommitHead(rs2, "main", sum2, c2))
	url2 := "https://acme.remote"
	apitest.RegisterHandlerWithOrigin(url2, http.MethodGet, "/info/refs/", api.NewInfoRefsHandler(rs2))
	apitest.RegisterHandlerWithOrigin(url2, http.MethodPost, "/upload-pack/", api.NewUploadPackHandler(db2, rs2, 0))

	db3 := objmock.NewStore()
	rs3 := refmock.NewStore()
	sum3, c3 := factory.CommitRandom(t, db3, nil)
	require.NoError(t, ref.CommitHead(rs3, "main", sum3, c3))
	url3 := "https://home.remote"
	apitest.RegisterHandlerWithOrigin(url3, http.MethodGet, "/info/refs/", api.NewInfoRefsHandler(rs3))
	apitest.RegisterHandlerWithOrigin(url3, http.MethodPost, "/upload-pack/", api.NewUploadPackHandler(db3, rs3, 0))

	rd, cleanUp := createRepoDir(t)
	rs := rd.OpenRefStore()
	defer cleanUp()

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", url1})
	assertCommandNoErr(t, cmd)
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "acme", url2})
	assertCommandNoErr(t, cmd)
	cmd = newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "home", url3})
	assertCommandNoErr(t, cmd)

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "acme"})
	assertCommandNoErr(t, cmd)
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	sum, err := ref.GetRemoteRef(rs, "acme", "main")
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	_, err = ref.GetRemoteRef(rs, "origin", "main")
	assert.Equal(t, ref.ErrKeyNotFound, err)
	_, err = ref.GetRemoteRef(rs, "home", "main")
	assert.Equal(t, ref.ErrKeyNotFound, err)
	apitest.AssertCommitsPersisted(t, db, [][]byte{sum2})
	require.NoError(t, db.Close())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "--all"})
	assertCommandNoErr(t, cmd)
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	sum, err = ref.GetRemoteRef(rs, "origin", "main")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	sum, err = ref.GetRemoteRef(rs, "home", "main")
	require.NoError(t, err)
	assert.Equal(t, sum3, sum)
	apitest.AssertCommitsPersisted(t, db, [][]byte{sum1, sum3})
	require.NoError(t, db.Close())
}

func TestFetchCmdCustomRefSpec(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()

	db1 := objmock.NewStore()
	rs1 := refmock.NewStore()
	sum1, _ := factory.CommitRandom(t, db1, nil)
	require.NoError(t, ref.SaveRef(rs1, "custom/abc", sum1, "test", "test@domain.com", "test", "test fetch custom"))
	apitest.RegisterHandler(http.MethodGet, "/info/refs/", api.NewInfoRefsHandler(rs1))
	apitest.RegisterHandler(http.MethodPost, "/upload-pack/", api.NewUploadPackHandler(db1, rs1, 0))

	rd, cleanUp := createRepoDir(t)
	rs := rd.OpenRefStore()
	defer cleanUp()

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", apitest.TestOrigin})
	assertCommandNoErr(t, cmd)

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "origin", "refs/custom/abc:refs/custom/abc"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + apitest.TestOrigin,
		" * [new ref]         refs/custom/abc -> refs/custom/abc",
		"",
	}, "\n"))
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	sum, err := ref.GetRef(rs, "custom/abc")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	apitest.AssertCommitsPersisted(t, db, [][]byte{sum1})
	refhelpers.AssertLatestReflogEqual(t, rs, "custom/abc", &ref.Reflog{
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

	db1 := objmock.NewStore()
	rs1 := refmock.NewStore()
	sum1, _ := factory.CommitRandom(t, db1, nil)
	require.NoError(t, ref.SaveTag(rs1, "2020-dec", sum1))
	sum2, _ := factory.CommitRandom(t, db1, nil)
	require.NoError(t, ref.SaveTag(rs1, "2021-dec", sum2))
	apitest.RegisterHandler(http.MethodGet, "/info/refs/", api.NewInfoRefsHandler(rs1))
	apitest.RegisterHandler(http.MethodPost, "/upload-pack/", api.NewUploadPackHandler(db1, rs1, 0))

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum3, _ := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.SaveTag(rs, "2020-dec", sum3))
	sum4, _ := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.SaveTag(rs, "2021-dec", sum4))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", apitest.TestOrigin})
	assertCommandNoErr(t, cmd)

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "origin", "refs/tags/202*:refs/tags/202*"})
	assertCmdFailed(t, cmd, strings.Join([]string{
		"From " + apitest.TestOrigin,
		" ! [rejected]        2020-dec    -> 2020-dec (would clobber existing tag)",
		" ! [rejected]        2021-dec    -> 2021-dec (would clobber existing tag)",
		"",
	}, "\n"), fmt.Errorf("failed to fetch some refs from "+apitest.TestOrigin))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "origin", "+refs/tags/2020*:refs/tags/2020*"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + apitest.TestOrigin,
		" t [tag update]      2020-dec    -> 2020-dec",
		"",
	}, "\n"))
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	sum, err := ref.GetTag(rs, "2020-dec")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	apitest.AssertCommitsPersisted(t, db, [][]byte{sum1})
	refhelpers.AssertLatestReflogEqual(t, rs, "tags/2020-dec", &ref.Reflog{
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
		"From " + apitest.TestOrigin,
		" t [tag update]      2021-dec    -> 2021-dec",
		"",
	}, "\n"))
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	sum, err = ref.GetTag(rs, "2021-dec")
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	apitest.AssertCommitsPersisted(t, db, [][]byte{sum2})
	refhelpers.AssertLatestReflogEqual(t, rs, "tags/2021-dec", &ref.Reflog{
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

	db1 := objmock.NewStore()
	rs1 := refmock.NewStore()
	sum1, c1 := factory.CommitRandom(t, db1, nil)
	require.NoError(t, ref.CommitHead(rs1, "abc", sum1, c1))
	apitest.RegisterHandler(http.MethodGet, "/info/refs/", api.NewInfoRefsHandler(rs1))
	apitest.RegisterHandler(http.MethodPost, "/upload-pack/", api.NewUploadPackHandler(db1, rs1, 0))

	rd, cleanUp := createRepoDir(t)
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	defer cleanUp()
	rs := rd.OpenRefStore()
	sum2, c2 := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.CommitHead(rs, "abc", sum2, c2))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", apitest.TestOrigin})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "origin", "refs/heads/abc:refs/heads/abc"})
	assertCmdFailed(t, cmd, strings.Join([]string{
		"From " + apitest.TestOrigin,
		" ! [rejected]        abc         -> abc (non-fast-forward)",
		"",
	}, "\n"), fmt.Errorf("failed to fetch some refs from "+apitest.TestOrigin))

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "origin", "+refs/heads/abc:refs/heads/abc"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + apitest.TestOrigin,
		fmt.Sprintf(" + %s...%s abc         -> abc (forced update)", hex.EncodeToString(sum2)[:7], hex.EncodeToString(sum1)[:7]),
		"",
	}, "\n"))

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	sum, err := ref.GetHead(rs, "abc")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	apitest.AssertCommitsPersisted(t, db, [][]byte{sum1})
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/abc", &ref.Reflog{
		OldOID:      sum2,
		NewOID:      sum1,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "forced-update",
	})
}
