package pack

import (
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	packclient "github.com/wrgl/core/pkg/pack/client"
	packtest "github.com/wrgl/core/pkg/pack/test"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/pkg/versioning"
)

func TestInfoRefs(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	sum1, commit1 := versioning.SaveTestCommit(t, db, nil)
	head := "my-branch"
	err := versioning.CommitHead(db, fs, head, sum1, commit1)
	require.NoError(t, err)
	sum2, _ := versioning.SaveTestCommit(t, db, nil)
	tag := "my-tag"
	err = versioning.SaveTag(db, tag, sum2)
	require.NoError(t, err)
	sum3, _ := versioning.SaveTestCommit(t, db, nil)
	remote := "origin"
	name := "main"
	err = versioning.SaveRemoteRef(
		db, fs, remote, name, sum3,
		testutils.BrokenRandomAlphaNumericString(8),
		testutils.BrokenRandomAlphaNumericString(10),
		"fetch",
		"from origin",
	)
	require.NoError(t, err)
	packtest.RegisterHandler(http.MethodGet, "/info/refs/", NewInfoRefsHandler(db))

	c, err := packclient.NewClient(packtest.TestOrigin)
	require.NoError(t, err)
	m, err := c.GetRefsInfo()
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"refs/heads/" + head: sum1,
		"refs/tags/" + tag:   sum2,
	}, m)
}
