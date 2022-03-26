package server_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func (s *testSuite) TestGetRefsHandler(t *testing.T) {
	repo, cli, m, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()
	db := s.s.GetDB(repo)
	rs := s.s.GetRS(repo)
	sum1, commit1 := refhelpers.SaveTestCommit(t, db, nil)
	head := "my-branch"
	err := ref.CommitHead(rs, head, sum1, commit1, nil)
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

	refs, err := cli.GetRefs("")
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"heads/" + head: sum1,
		"tags/" + tag:   sum2,
	}, refs)

	refs, err = cli.GetRefs("heads/")
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"heads/" + head: sum1,
	}, refs)

	// pass custom header
	req := m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "sdf")
		refs, err := cli.GetRefs("", apiclient.WithRequestHeader(header))
		require.NoError(t, err)
		assert.Greater(t, len(refs), 0)
	})
	assert.Equal(t, "sdf", req.Header.Get("Custom-Header"))
}
