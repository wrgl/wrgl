// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/api/payload"
	confhelpers "github.com/wrgl/wrgl/pkg/conf/helpers"
	"github.com/wrgl/wrgl/pkg/credentials"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	server_testutils "github.com/wrgl/wrgl/wrgld/pkg/server/testutils"
)

func authenticate(t *testing.T, ts *server_testutils.Server, uri string, scopes ...string) {
	t.Helper()
	cs, err := credentials.NewStore()
	require.NoError(t, err)
	var tok string
	if len(scopes) == 0 {
		tok = ts.AdminToken(t)
	} else {
		tok = ts.Authorize(t, server_testutils.Email, server_testutils.Name, scopes...)
	}
	u, err := url.Parse(uri)
	require.NoError(t, err)
	cs.Set(*u, tok)
	require.NoError(t, cs.Flush())
}

func unauthenticate(t *testing.T, uri string) {
	t.Helper()
	cs, err := credentials.NewStore()
	require.NoError(t, err)
	u, err := url.Parse(uri)
	require.NoError(t, err)
	cs.Delete(*u)
	require.NoError(t, cs.Flush())
}

func httpError(t *testing.T, code int, message string) *apiclient.HTTPError {
	err := &apiclient.HTTPError{
		Code: code,
		Body: &payload.Error{
			Message: message,
		},
	}
	b, merr := json.Marshal(err.Body)
	require.NoError(t, merr)
	err.RawBody = b
	return err
}

func assertCmdUnauthorized(t *testing.T, cmd *cobra.Command, url string) {
	t.Helper()
	assertCmdFailed(t, cmd, strings.Join([]string{
		fmt.Sprintf("No credential found for %s", url),
		"Proceed as anonymous user...",
		"Unauthorized.",
		"Run this command to authenticate:",
		fmt.Sprintf("    wrgl credentials authenticate %s", url),
		"",
	}, "\n"), httpError(t, http.StatusForbidden, "Forbidden"))
}

func TestFetchCmd(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := server_testutils.NewServer(t, nil)
	repo, url, _, cleanup := ts.NewRemote(t, "", nil)
	defer cleanup()
	dbs := ts.GetDB(repo)
	rss := ts.GetRS(repo)
	sum1, c1 := factory.CommitRandom(t, dbs, nil)
	sum2, c2 := factory.CommitRandom(t, dbs, [][]byte{sum1})
	sum3, c3 := factory.CommitRandom(t, dbs, nil)
	sum4, c4 := factory.CommitRandom(t, dbs, [][]byte{sum3})
	require.NoError(t, ref.CommitHead(rss, "main", sum2, c2))
	require.NoError(t, ref.CommitHead(rss, "tickets", sum4, c4))
	require.NoError(t, ref.SaveTag(rss, "2020", sum1))

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	factory.CopyCommitsToNewStore(t, dbs, db, [][]byte{sum1, sum3})
	require.NoError(t, ref.CommitHead(rs, "main", sum1, c1))
	require.NoError(t, ref.CommitHead(rs, "tickets", sum3, c3))
	require.NoError(t, db.Close())

	cmd := rootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", url})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"fetch"})
	assertCmdUnauthorized(t, cmd, url)

	authenticate(t, ts, url)

	cmd = rootCmd()
	cmd.SetArgs([]string{"fetch", "--no-progress"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + url,
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
	factory.AssertCommitsPersisted(t, db, [][]byte{sum2, sum4})
	refhelpers.AssertLatestReflogEqual(t, rs, "remotes/origin/main", &ref.Reflog{
		NewOID:      sum2,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "[from origin] storing head",
	})
	refhelpers.AssertLatestReflogEqual(t, rs, "remotes/origin/tickets", &ref.Reflog{
		NewOID:      sum4,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "[from origin] storing head",
	})
	require.NoError(t, db.Close())

	cmd = rootCmd()
	cmd.SetArgs([]string{"fetch"})
	assertCmdOutput(t, cmd, "")
}

func assertCommandNoErr(t *testing.T, cmd *cobra.Command) {
	t.Helper()
	require.NoError(t, cmd.Execute())
}

func TestFetchCmdAllRepos(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := server_testutils.NewServer(t, nil)
	repo, url1, _, cleanup := ts.NewRemote(t, "", nil)
	defer cleanup()
	db1 := ts.GetDB(repo)
	rs1 := ts.GetRS(repo)
	sum1, c1 := factory.CommitRandom(t, db1, nil)
	require.NoError(t, ref.CommitHead(rs1, "main", sum1, c1))

	repo, url2, _, cleanup := ts.NewRemote(t, "", nil)
	defer cleanup()
	db2 := ts.GetDB(repo)
	rs2 := ts.GetRS(repo)
	sum2, c2 := factory.CommitRandom(t, db2, nil)
	require.NoError(t, ref.CommitHead(rs2, "main", sum2, c2))

	repo, url3, _, cleanup := ts.NewRemote(t, "", nil)
	defer cleanup()
	db3 := ts.GetDB(repo)
	rs3 := ts.GetRS(repo)
	sum3, c3 := factory.CommitRandom(t, db3, nil)
	require.NoError(t, ref.CommitHead(rs3, "main", sum3, c3))

	rd, cleanUp := createRepoDir(t)
	rs := rd.OpenRefStore()
	defer cleanUp()

	cmd := rootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", url1})
	assertCommandNoErr(t, cmd)
	cmd = rootCmd()
	cmd.SetArgs([]string{"remote", "add", "acme", url2})
	assertCommandNoErr(t, cmd)
	cmd = rootCmd()
	cmd.SetArgs([]string{"remote", "add", "home", url3})
	assertCommandNoErr(t, cmd)

	authenticate(t, ts, url1)
	authenticate(t, ts, url2)
	authenticate(t, ts, url3)

	cmd = rootCmd()
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
	factory.AssertCommitsPersisted(t, db, [][]byte{sum2})
	require.NoError(t, db.Close())

	cmd = rootCmd()
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
	factory.AssertCommitsPersisted(t, db, [][]byte{sum1, sum3})
	require.NoError(t, db.Close())
}

func TestFetchCmdCustomRefSpec(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := server_testutils.NewServer(t, nil)
	repo, url, _, cleanup := ts.NewRemote(t, "", nil)
	defer cleanup()
	db1 := ts.GetDB(repo)
	rs1 := ts.GetRS(repo)
	sum1, _ := factory.CommitRandom(t, db1, nil)
	require.NoError(t, ref.SaveRef(rs1, "custom/abc", sum1, "test", "test@domain.com", "test", "test fetch custom"))

	rd, cleanUp := createRepoDir(t)
	rs := rd.OpenRefStore()
	defer cleanUp()

	cmd := rootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", url})
	assertCommandNoErr(t, cmd)
	authenticate(t, ts, url)

	cmd = rootCmd()
	cmd.SetArgs([]string{"fetch", "--no-progress", "origin", "refs/custom/abc:refs/custom/abc"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + url,
		" * [new ref]         refs/custom/abc -> refs/custom/abc",
		"",
	}, "\n"))
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	sum, err := ref.GetRef(rs, "custom/abc")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	factory.AssertCommitsPersisted(t, db, [][]byte{sum1})
	refhelpers.AssertLatestReflogEqual(t, rs, "custom/abc", &ref.Reflog{
		NewOID:      sum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "[from origin] storing ref",
	})
}

func TestFetchCmdTag(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := server_testutils.NewServer(t, nil)
	repo, url, _, cleanup := ts.NewRemote(t, "", nil)
	defer cleanup()
	db1 := ts.GetDB(repo)
	rs1 := ts.GetRS(repo)
	sum1, _ := factory.CommitRandom(t, db1, nil)
	require.NoError(t, ref.SaveTag(rs1, "2020-dec", sum1))
	sum2, _ := factory.CommitRandom(t, db1, nil)
	require.NoError(t, ref.SaveTag(rs1, "2021-dec", sum2))

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

	cmd := rootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", url})
	assertCommandNoErr(t, cmd)

	authenticate(t, ts, url)

	cmd = rootCmd()
	cmd.SetArgs([]string{"fetch", "--no-progress", "origin", "refs/tags/202*:refs/tags/202*"})
	assertCmdFailed(t, cmd, strings.Join([]string{
		"From " + url,
		" ! [rejected]        2020-dec    -> 2020-dec (would clobber existing tag)",
		" ! [rejected]        2021-dec    -> 2021-dec (would clobber existing tag)",
		"",
	}, "\n"), fmt.Errorf("failed to fetch some refs from "+url))

	cmd = rootCmd()
	cmd.SetArgs([]string{"fetch", "--no-progress", "origin", "+refs/tags/2020*:refs/tags/2020*"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + url,
		" t [tag update]      2020-dec    -> 2020-dec",
		"",
	}, "\n"))
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	sum, err := ref.GetTag(rs, "2020-dec")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	factory.AssertCommitsPersisted(t, db, [][]byte{sum1})
	refhelpers.AssertLatestReflogEqual(t, rs, "tags/2020-dec", &ref.Reflog{
		OldOID:      sum3,
		NewOID:      sum1,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "[from origin] updating tag",
	})
	require.NoError(t, db.Close())

	cmd = rootCmd()
	cmd.SetArgs([]string{"fetch", "--no-progress", "origin", "refs/tags/2021*:refs/tags/2021*", "--force"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + url,
		" t [tag update]      2021-dec    -> 2021-dec",
		"",
	}, "\n"))
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	sum, err = ref.GetTag(rs, "2021-dec")
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	factory.AssertCommitsPersisted(t, db, [][]byte{sum2})
	refhelpers.AssertLatestReflogEqual(t, rs, "tags/2021-dec", &ref.Reflog{
		OldOID:      sum4,
		NewOID:      sum2,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "[from origin] updating tag",
	})
	require.NoError(t, db.Close())
}

func TestFetchCmdForceUpdate(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := server_testutils.NewServer(t, nil)
	repo, url, _, cleanup := ts.NewRemote(t, "", nil)
	defer cleanup()
	db1 := ts.GetDB(repo)
	rs1 := ts.GetRS(repo)
	sum1, c1 := factory.CommitRandom(t, db1, nil)
	require.NoError(t, ref.CommitHead(rs1, "abc", sum1, c1))

	rd, cleanUp := createRepoDir(t)
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	defer cleanUp()
	rs := rd.OpenRefStore()
	sum2, c2 := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.CommitHead(rs, "abc", sum2, c2))
	require.NoError(t, db.Close())

	cmd := rootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", url})
	require.NoError(t, cmd.Execute())

	authenticate(t, ts, url)

	cmd = rootCmd()
	cmd.SetArgs([]string{"fetch", "--no-progress", "origin", "refs/heads/abc:refs/heads/abc"})
	assertCmdFailed(t, cmd, strings.Join([]string{
		"From " + url,
		" ! [rejected]        abc         -> abc (non-fast-forward)",
		"",
	}, "\n"), fmt.Errorf("failed to fetch some refs from "+url))

	cmd = rootCmd()
	cmd.SetArgs([]string{"fetch", "--no-progress", "origin", "+refs/heads/abc:refs/heads/abc"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + url,
		fmt.Sprintf(" + %s...%s abc         -> abc (forced update)", hex.EncodeToString(sum2)[:7], hex.EncodeToString(sum1)[:7]),
		"",
	}, "\n"))

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	defer db.Close()
	sum, err := ref.GetHead(rs, "abc")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	factory.AssertCommitsPersisted(t, db, [][]byte{sum1})
	refhelpers.AssertLatestReflogEqual(t, rs, "heads/abc", &ref.Reflog{
		OldOID:      sum2,
		NewOID:      sum1,
		AuthorName:  "John Doe",
		AuthorEmail: "john@domain.com",
		Action:      "fetch",
		Message:     "[from origin] forced-update",
	})
}

func TestFetchCmdDepth(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := server_testutils.NewServer(t, nil)
	repo, url, _, cleanup := ts.NewRemote(t, "", nil)
	defer cleanup()
	dbs := ts.GetDB(repo)
	rss := ts.GetRS(repo)
	sum1, c1 := factory.CommitRandom(t, dbs, nil)
	sum2, c2 := factory.CommitRandom(t, dbs, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rss, "main", sum2, c2))

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()

	cmd := rootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", url})
	require.NoError(t, cmd.Execute())

	authenticate(t, ts, url)
	cmd = rootCmd()
	cmd.SetArgs([]string{"fetch", "--no-progress", "--depth", "1"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"From " + url,
		" * [new branch]      main        -> origin/main",
		"",
	}, "\n"))

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum, err := ref.GetRemoteRef(rs, "origin", "main")
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	factory.AssertCommitsShallowlyPersisted(t, db, [][]byte{sum2, sum1})
	factory.AssertTablesPersisted(t, db, [][]byte{c2.Table})
	factory.AssertTablesNotPersisted(t, db, [][]byte{c1.Table})
	require.NoError(t, db.Close())

	// fetch missing table
	cmd = rootCmd()
	cmd.SetArgs([]string{"fetch", "tables", "--no-progress", "origin", hex.EncodeToString(c1.Table)})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("Table %x persisted", c1.Table),
		"",
	}, "\n"))
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	factory.AssertTablesPersisted(t, db, [][]byte{c1.Table})
	require.NoError(t, db.Close())
}
