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
	"github.com/wrgl/core/pkg/pack"
	packtest "github.com/wrgl/core/pkg/pack/test"
	"github.com/wrgl/core/pkg/versioning"
)

func assertRefStore(t *testing.T, db kv.DB, name string, sum []byte) {
	t.Helper()
	b, err := versioning.GetRef(db, name)
	if sum == nil {
		assert.Empty(t, b)
	} else {
		require.NoError(t, err)
		assert.Equal(t, sum, b)
	}
}

func TestPushCmd(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	dbs := kv.NewMockStore(false)
	fss := kv.NewMockStore(false)
	sum1, c1 := factory.CommitHead(t, dbs, fss, "beta", nil, nil)
	sum10, _ := factory.CommitTag(t, dbs, fss, "2017", nil, nil, nil)
	sum6, _ := factory.CommitTag(t, dbs, fss, "2018", nil, nil, nil)
	factory.CommitTag(t, dbs, fss, "2019", nil, nil, nil)
	sum4, _ := factory.CommitHead(t, dbs, fss, "gamma", nil, nil)
	sum9, _ := factory.CommitHead(t, dbs, fss, "delta", nil, nil)
	sum8, c8 := factory.CommitHead(t, dbs, fss, "theta", nil, nil)
	packtest.RegisterHandler(http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(dbs))
	packtest.RegisterHandler(
		http.MethodPost, "/receive-pack/", pack.NewReceivePackHandler(dbs, fss, packtest.ReceivePackConfig(false, false)),
	)

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	packtest.CopyCommitsToNewStore(t, dbs, db, fss, fs, [][]byte{sum1, sum8})
	sum11, _ := factory.CommitHead(t, db, fs, "alpha", nil, nil)
	require.NoError(t, versioning.CommitHead(db, fs, "beta", sum1, c1))
	require.NoError(t, versioning.CommitHead(db, fs, "theta", sum8, c8))
	sum2, _ := factory.CommitHead(t, db, fs, "beta", nil, nil)
	factory.CommitTag(t, db, fs, "2017", nil, nil, nil)
	sum7, _ := factory.CommitTag(t, db, fs, "2018", nil, nil, nil)
	sum15, _ := factory.CommitTag(t, db, fs, "2020", nil, nil, nil)
	sum5, _ := factory.CommitHead(t, db, fs, "gamma", nil, nil)
	factory.CommitHead(t, db, fs, "delta", nil, nil)
	sum3, _ := factory.CommitRandom(t, db, fs, nil)
	require.NoError(t, versioning.SaveRef(db, fs, "refs/custom/abc", sum3, "test", "test@domain.com", "test", "test push"))
	sum12, _ := factory.CommitHead(t, db, fs, "xi", nil, nil)
	sum13, _ := factory.CommitTag(t, db, fs, "omega", nil, nil, nil)
	sum14, _ := factory.CommitTag(t, db, fs, "epsilon", nil, nil, nil)
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "my-repo", packtest.TestOrigin})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
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
		" ! [rejected]          delta       -> delta (non-fast-forward)",
		" ! [rejected]          2017        -> 2017 (would clobber existing tag)",
		" = [up to date]        theta       -> theta",
		" * [new branch]        alpha       -> alpha",
		" * [new branch]        xi          -> xi",
		" * [new tag]           omega       -> omega",
		" * [new tag]           epsilon     -> epsilon",
		fmt.Sprintf("   %s..%s    beta        -> beta", hex.EncodeToString(sum1)[:7], hex.EncodeToString(sum2)[:7]),
		" * [new tag]           2020        -> 2020",
		fmt.Sprintf(" + %s...%s   2018        -> 2018 (forced update)", hex.EncodeToString(sum6)[:7], hex.EncodeToString(sum7)[:7]),
		" * [new reference]     refs/custom/abc -> refs/custom/abc",
		" - [deleted]                       -> 2019",
		fmt.Sprintf(" + %s...%s   gamma       -> gamma (forced update)", hex.EncodeToString(sum4)[:7], hex.EncodeToString(sum5)[:7]),
		"",
	}, "\n"))

	packtest.AssertCommitsPersisted(t, dbs, fss, [][]byte{sum2, sum3, sum7, sum5, sum11, sum12, sum13, sum14, sum15})
	assertRefStore(t, dbs, "heads/beta", sum2)
	assertRefStore(t, dbs, "tags/2018", sum7)
	assertRefStore(t, dbs, "heads/gamma", sum5)
	assertRefStore(t, dbs, "tags/2017", sum10)
	assertRefStore(t, dbs, "tags/2019", nil)
	assertRefStore(t, dbs, "heads/delta", sum9)
	assertRefStore(t, dbs, "heads/theta", sum8)
	assertRefStore(t, dbs, "heads/alpha", sum11)
	assertRefStore(t, dbs, "heads/xi", sum12)
	assertRefStore(t, dbs, "tags/omega", sum13)
	assertRefStore(t, dbs, "tags/epsilon", sum14)
	assertRefStore(t, dbs, "tags/2020", sum15)
	assertRefStore(t, dbs, "custom/abc", sum3)
}

func TestPushCmdForce(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	dbs := kv.NewMockStore(false)
	fss := kv.NewMockStore(false)
	sum1, _ := factory.CommitHead(t, dbs, fss, "alpha", nil, nil)
	sum5, _ := factory.CommitHead(t, dbs, fss, "beta", nil, nil)
	sum2, _ := factory.CommitTag(t, dbs, fss, "2017", nil, nil, nil)
	packtest.RegisterHandler(http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(dbs))
	packtest.RegisterHandler(
		http.MethodPost, "/receive-pack/", pack.NewReceivePackHandler(dbs, fss, packtest.ReceivePackConfig(false, true)),
	)

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	sum3, _ := factory.CommitHead(t, db, fs, "alpha", nil, nil)
	sum4, _ := factory.CommitTag(t, db, fs, "2017", nil, nil, nil)
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", packtest.TestOrigin})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{
		"push", "--force",
		"refs/heads/alpha:",
		":refs/heads/beta",
		"refs/tags/2017:",
	})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf(" + %s...%s   alpha       -> alpha (forced update)", hex.EncodeToString(sum1)[:7], hex.EncodeToString(sum3)[:7]),
		" ! [remote rejected]               -> beta (remote does not support deleting refs)",
		fmt.Sprintf(" + %s...%s   2017        -> 2017 (forced update)", hex.EncodeToString(sum2)[:7], hex.EncodeToString(sum4)[:7]),
		"",
	}, "\n"))

	packtest.AssertCommitsPersisted(t, dbs, fss, [][]byte{sum3, sum4})
	assertRefStore(t, dbs, "heads/alpha", sum3)
	assertRefStore(t, dbs, "tags/2017", sum4)
	assertRefStore(t, dbs, "heads/beta", sum5)
}
