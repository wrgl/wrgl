// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apitest "github.com/wrgl/core/pkg/api/test"
	"github.com/wrgl/core/pkg/conf"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	confhelpers "github.com/wrgl/core/pkg/conf/helpers"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/ref"
)

func assertRefStore(t *testing.T, rs ref.Store, name string, sum []byte) {
	t.Helper()
	b, err := ref.GetRef(rs, name)
	if sum == nil {
		assert.Empty(t, b)
	} else {
		require.NoError(t, err)
		assert.Equal(t, sum, b)
	}
}

func TestPushCmd(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := apitest.NewServer(t, nil)
	repo, url, _, cleanup := ts.NewRemote(t, true, "", nil)
	defer cleanup()
	dbs := ts.GetDB(repo)
	rss := ts.GetRS(repo)
	sum1, c1 := factory.CommitHead(t, dbs, rss, "beta", nil, nil)
	sum10, _ := factory.CommitTag(t, dbs, rss, "2017", nil, nil, nil)
	sum6, _ := factory.CommitTag(t, dbs, rss, "2018", nil, nil, nil)
	factory.CommitTag(t, dbs, rss, "2019", nil, nil, nil)
	sum4, _ := factory.CommitHead(t, dbs, rss, "gamma", nil, nil)
	sum9, _ := factory.CommitHead(t, dbs, rss, "delta", nil, nil)
	sum8, c8 := factory.CommitHead(t, dbs, rss, "theta", nil, nil)

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	apitest.CopyCommitsToNewStore(t, dbs, db, [][]byte{sum1, sum8})
	sum11, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	require.NoError(t, ref.CommitHead(rs, "beta", sum1, c1))
	require.NoError(t, ref.CommitHead(rs, "theta", sum8, c8))
	sum2, _ := factory.CommitHead(t, db, rs, "beta", nil, nil)
	factory.CommitTag(t, db, rs, "2017", nil, nil, nil)
	sum7, _ := factory.CommitTag(t, db, rs, "2018", nil, nil, nil)
	sum15, _ := factory.CommitTag(t, db, rs, "2020", nil, nil, nil)
	sum5, _ := factory.CommitHead(t, db, rs, "gamma", nil, nil)
	factory.CommitHead(t, db, rs, "delta", nil, nil)
	sum3, _ := factory.CommitRandom(t, db, nil)
	require.NoError(t, ref.SaveRef(rs, "refs/custom/abc", sum3, "test", "test@domain.com", "test", "test push"))
	sum12, _ := factory.CommitHead(t, db, rs, "xi", nil, nil)
	sum13, _ := factory.CommitTag(t, db, rs, "omega", nil, nil, nil)
	sum14, _ := factory.CommitTag(t, db, rs, "epsilon", nil, nil, nil)
	require.NoError(t, db.Close())

	cmd := RootCmd()
	cmd.SetArgs([]string{"remote", "add", "my-repo", url})
	require.NoError(t, cmd.Execute())

	authenticate(t, url)
	cmd = RootCmd()
	cmd.SetArgs([]string{
		"push", "my-repo",
		"refs/heads/delta:",
		"refs/heads/alpha:alpha",
		"refs/heads/xi:heads/xi",
		"refs/tags/omega:omega",
		"refs/tags/epsilon:tags/epsilon",
		"refs/heads/beta:beta",
		"refs/tags/2020:",
		"refs/tags/2017:",
		"+refs/tags/2018:",
		"refs/custom/abc:refs/custom/abc",
		":refs/tags/2019",
		"+refs/heads/gamma:",
		"theta:theta",
	})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("To %s", url),
		" ! [rejected]        delta       -> delta (non-fast-forward)",
		" ! [rejected]        2017        -> 2017 (would clobber existing tag)",
		" = [up to date]      theta       -> theta",
		" * [new branch]      alpha       -> alpha",
		" * [new branch]      xi          -> xi",
		" * [new tag]         omega       -> omega",
		" * [new tag]         epsilon     -> epsilon",
		fmt.Sprintf("   %s..%s  beta        -> beta", hex.EncodeToString(sum1)[:7], hex.EncodeToString(sum2)[:7]),
		" * [new tag]         2020        -> 2020",
		fmt.Sprintf(" + %s...%s 2018        -> 2018 (forced update)", hex.EncodeToString(sum6)[:7], hex.EncodeToString(sum7)[:7]),
		" * [new reference]   refs/custom/abc -> refs/custom/abc",
		" - [deleted]                     -> 2019",
		fmt.Sprintf(" + %s...%s gamma       -> gamma (forced update)", hex.EncodeToString(sum4)[:7], hex.EncodeToString(sum5)[:7]),
		"",
	}, "\n"))

	apitest.AssertCommitsPersisted(t, dbs, [][]byte{sum2, sum3, sum7, sum5, sum11, sum12, sum13, sum14, sum15})
	assertRefStore(t, rss, "heads/beta", sum2)
	assertRefStore(t, rss, "tags/2018", sum7)
	assertRefStore(t, rss, "heads/gamma", sum5)
	assertRefStore(t, rss, "tags/2017", sum10)
	assertRefStore(t, rss, "tags/2019", nil)
	assertRefStore(t, rss, "heads/delta", sum9)
	assertRefStore(t, rss, "heads/theta", sum8)
	assertRefStore(t, rss, "heads/alpha", sum11)
	assertRefStore(t, rss, "heads/xi", sum12)
	assertRefStore(t, rss, "tags/omega", sum13)
	assertRefStore(t, rss, "tags/epsilon", sum14)
	assertRefStore(t, rss, "tags/2020", sum15)
	assertRefStore(t, rss, "custom/abc", sum3)
}

func TestPushCmdForce(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := apitest.NewServer(t, nil)
	repo, url, _, cleanup := ts.NewRemote(t, true, "", nil)
	defer cleanup()
	dbs := ts.GetDB(repo)
	rss := ts.GetRS(repo)
	cs := ts.GetConfS(repo)
	require.NoError(t, cs.Save(apitest.ReceivePackConfig(false, true)))
	sum1, _ := factory.CommitHead(t, dbs, rss, "alpha", nil, nil)
	sum5, _ := factory.CommitHead(t, dbs, rss, "beta", nil, nil)
	sum2, _ := factory.CommitTag(t, dbs, rss, "2017", nil, nil, nil)

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum3, _ := factory.CommitHead(t, db, rs, "alpha", nil, nil)
	sum4, _ := factory.CommitTag(t, db, rs, "2017", nil, nil, nil)
	require.NoError(t, db.Close())

	cmd := RootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", url})
	require.NoError(t, cmd.Execute())
	for _, ref := range []string{
		"refs/heads/alpha:refs/heads/alpha",
		":refs/heads/beta",
		"refs/tags/2017:refs/tags/2017",
	} {
		cmd = RootCmd()
		cmd.SetArgs([]string{"config", "add", "remote.origin.push", ref})
		require.NoError(t, cmd.Execute())
	}

	authenticate(t, url)
	cmd = RootCmd()
	cmd.SetArgs([]string{"push", "--force"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("To %s", url),
		fmt.Sprintf(" + %s...%s alpha       -> alpha (forced update)", hex.EncodeToString(sum1)[:7], hex.EncodeToString(sum3)[:7]),
		" ! [remote rejected]             -> beta (remote does not support deleting refs)",
		fmt.Sprintf(" + %s...%s 2017        -> 2017 (forced update)", hex.EncodeToString(sum2)[:7], hex.EncodeToString(sum4)[:7]),
		"",
	}, "\n"))

	apitest.AssertCommitsPersisted(t, dbs, [][]byte{sum3, sum4})
	assertRefStore(t, rss, "heads/alpha", sum3)
	assertRefStore(t, rss, "tags/2017", sum4)
	assertRefStore(t, rss, "heads/beta", sum5)
}

func TestPushCmdSetUpstream(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := apitest.NewServer(t, nil)
	repo, url, _, cleanup := ts.NewRemote(t, true, "", nil)
	defer cleanup()
	dbs := ts.GetDB(repo)
	rss := ts.GetRS(repo)
	cs := ts.GetConfS(repo)
	require.NoError(t, cs.Save(apitest.ReceivePackConfig(false, true)))
	sum1, c1 := factory.CommitHead(t, dbs, rss, "alpha", nil, nil)

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	apitest.CopyCommitsToNewStore(t, dbs, db, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rs, "alpha", sum1, c1))
	factory.CommitHead(t, db, rs, "beta", nil, nil)
	require.NoError(t, db.Close())

	cmd := RootCmd()
	cmd.SetArgs([]string{"remote", "add", "my-repo", url})
	require.NoError(t, cmd.Execute())

	authenticate(t, url)
	cmd = RootCmd()
	cmd.SetArgs([]string{
		"push", "my-repo", "--set-upstream",
		"refs/heads/alpha:",
		"refs/heads/beta:",
	})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("To %s", url),
		" = [up to date]      alpha       -> alpha",
		" * [new branch]      beta        -> beta",
		"branch \"alpha\" setup to track remote branch \"alpha\" from \"my-repo\"",
		"branch \"beta\" setup to track remote branch \"beta\" from \"my-repo\"",
		"",
	}, "\n"))

	cs = conffs.NewStore(rd.FullPath, conffs.LocalSource, "")
	c, err := cs.Open()
	require.NoError(t, err)
	assert.Equal(t, map[string]*conf.Branch{
		"alpha": {
			Remote: "my-repo",
			Merge:  "refs/heads/alpha",
		},
		"beta": {
			Remote: "my-repo",
			Merge:  "refs/heads/beta",
		},
	}, c.Branch)
}

func TestPushCmdDepthGreaterThanOne(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := apitest.NewServer(t, nil)
	repo, url, _, cleanup := ts.NewRemote(t, true, "", nil)
	defer cleanup()
	dbs := ts.GetDB(repo)
	rss := ts.GetRS(repo)

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	sum1, _ := factory.CommitRandom(t, db, nil)
	t.Logf("sum1: %x", sum1)
	sum2, c2 := factory.CommitRandom(t, db, [][]byte{sum1})
	t.Logf("sum2: %x", sum2)
	require.NoError(t, ref.CommitHead(rs, "alpha", sum2, c2))
	require.NoError(t, db.Close())

	cmd := RootCmd()
	cmd.SetArgs([]string{"remote", "add", "my-repo", url})
	require.NoError(t, cmd.Execute())

	cmd = RootCmd()
	cmd.SetArgs([]string{"push", "my-repo", "refs/heads/alpha:"})
	assertCmdUnauthorized(t, cmd, url)
	authenticate(t, url)

	cmd = RootCmd()
	cmd.SetArgs([]string{"push", "my-repo", "refs/heads/alpha:"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("To %s", url),
		" * [new branch]      alpha       -> alpha",
		"",
	}, "\n"))

	apitest.AssertCommitsPersisted(t, dbs, [][]byte{sum1, sum2})
	assertRefStore(t, rss, "heads/alpha", sum2)
}
