// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/core/pkg/api/client"
	"github.com/wrgl/core/pkg/ref"
	refhelpers "github.com/wrgl/core/pkg/ref/helpers"
	"github.com/wrgl/core/pkg/testutils"
)

func (s *testSuite) TestGetRefsHandler(t *testing.T) {
	repo, cli, m, cleanup := s.NewClient(t)
	defer cleanup()
	db := s.getDB(repo)
	rs := s.getRS(repo)
	sum1, commit1 := refhelpers.SaveTestCommit(t, db, nil)
	head := "my-branch"
	err := ref.CommitHead(rs, head, sum1, commit1)
	require.NoError(t, err)
	sum2, _ := refhelpers.SaveTestCommit(t, db, nil)
	tag := "my-tag"
	err = ref.SaveTag(rs, tag, sum2)
	require.NoError(t, err)
	sum3, _ := refhelpers.SaveTestCommit(t, db, nil)
	remote := "origin"
	name := "main"
	err = ref.SaveRemoteRef(
		rs, remote, name, sum3,
		testutils.BrokenRandomAlphaNumericString(8),
		testutils.BrokenRandomAlphaNumericString(10),
		"fetch",
		"from origin",
	)
	require.NoError(t, err)

	refs, err := cli.GetRefs()
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"heads/" + head: sum1,
		"tags/" + tag:   sum2,
	}, refs)

	// pass custom header
	req := m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "sdf")
		refs, err := cli.GetRefs(apiclient.WithHeader(header))
		require.NoError(t, err)
		assert.Greater(t, len(refs), 0)
	})
	assert.Equal(t, "sdf", req.Header.Get("Custom-Header"))
}
