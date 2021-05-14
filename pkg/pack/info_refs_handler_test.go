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
	sum1 := testutils.SecureRandomBytes(16)
	head := "my-branch"
	err := versioning.SaveHead(db, head, sum1)
	require.NoError(t, err)
	sum2 := testutils.SecureRandomBytes(16)
	tag := "my-tag"
	err = versioning.SaveTag(db, tag, sum2)
	require.NoError(t, err)
	sum3 := testutils.SecureRandomBytes(16)
	remote := "origin"
	name := "main"
	err = versioning.SaveRemoteRef(db, remote, name, sum3)
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
