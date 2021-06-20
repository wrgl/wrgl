package main

import (
	"io"
	"net/http"
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

func TestPullCmd(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	dbs := kv.NewMockStore(false)
	fss := kv.NewMockStore(false)
	sum1, c1 := factory.CommitRandom(t, dbs, fss, nil)
	sum2, c2 := factory.CommitRandom(t, dbs, fss, [][]byte{sum1})
	require.NoError(t, versioning.CommitHead(dbs, fss, "main", sum2, c2))
	packtest.RegisterHandler(http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(dbs))
	packtest.RegisterHandler(http.MethodPost, "/upload-pack/", pack.NewUploadPackHandler(dbs, fss))

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	packtest.CopyCommitsToNewStore(t, dbs, db, fss, fs, [][]byte{sum1})
	require.NoError(t, versioning.CommitHead(db, fs, "main", sum1, c1))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", packtest.TestOrigin})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"pull", "main", "origin", "refs/heads/main:refs/remotes/origin/main", "--set-upstream"})
	cmd.SetOut(io.Discard)
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	sum, err := versioning.GetHead(db, "main")
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
}
