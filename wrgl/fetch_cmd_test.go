package main

import (
	"io"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/pack"
	packtest "github.com/wrgl/core/pkg/pack/test"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
)

func assertCommitsPersisted(t *testing.T, db kv.DB, fs kv.FileStore, commits [][]byte) {
	t.Helper()
	for _, sum := range commits {
		c, err := versioning.GetCommit(db, sum)
		require.NoError(t, err)
		tbl, err := table.ReadTable(db, fs, c.Table)
		require.NoError(t, err)
		reader := tbl.NewRowReader()
		for {
			_, _, err := reader.Read()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
	}
}

func TestFetchCmd(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	dbs := kv.NewMockStore(false)
	fss := kv.NewMockStore(false)
	sum1, _ := packtest.CreateCommit(t, dbs, fss, nil)
	sum2, _ := packtest.CreateCommit(t, dbs, fss, [][]byte{sum1})
	sum3, _ := packtest.CreateCommit(t, dbs, fss, nil)
	sum4, _ := packtest.CreateCommit(t, dbs, fss, [][]byte{sum3})
	require.NoError(t, versioning.SaveHead(dbs, "main", sum2))
	require.NoError(t, versioning.SaveHead(dbs, "tickets", sum4))
	packtest.RegisterHandler(http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(dbs))
	packtest.RegisterHandler(http.MethodPost, "/upload-pack/", pack.NewUploadPackHandler(dbs, fss))

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	fs := rd.OpenFileStore()
	packtest.CopyCommitsToNewStore(t, dbs, db, fss, fs, [][]byte{sum1, sum3})
	require.NoError(t, versioning.SaveHead(db, "main", sum1))
	require.NoError(t, versioning.SaveHead(db, "tickets", sum3))
	require.NoError(t, db.Close())

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", packtest.TestOrigin})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch"})
	require.NoError(t, cmd.Execute())
	db, err = rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	sum, err := versioning.GetRemoteRef(db, "origin", "main")
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	sum, err = versioning.GetRemoteRef(db, "origin", "tickets")
	require.NoError(t, err)
	assert.Equal(t, sum4, sum)
	assertCommitsPersisted(t, db, fs, [][]byte{sum2, sum4})
}

func TestFetchCmdAllRepos(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()

	db1 := kv.NewMockStore(false)
	fs1 := kv.NewMockStore(false)
	sum1, _ := packtest.CreateCommit(t, db1, fs1, nil)
	require.NoError(t, versioning.SaveHead(db1, "main", sum1))
	url1 := "https://origin.remote"
	packtest.RegisterHandlerWithOrigin(url1, http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(db1))
	packtest.RegisterHandlerWithOrigin(url1, http.MethodPost, "/upload-pack/", pack.NewUploadPackHandler(db1, fs1))

	db2 := kv.NewMockStore(false)
	fs2 := kv.NewMockStore(false)
	sum2, _ := packtest.CreateCommit(t, db2, fs2, nil)
	require.NoError(t, versioning.SaveHead(db2, "main", sum2))
	url2 := "https://acme.remote"
	packtest.RegisterHandlerWithOrigin(url2, http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(db2))
	packtest.RegisterHandlerWithOrigin(url2, http.MethodPost, "/upload-pack/", pack.NewUploadPackHandler(db2, fs2))

	db3 := kv.NewMockStore(false)
	fs3 := kv.NewMockStore(false)
	sum3, _ := packtest.CreateCommit(t, db3, fs3, nil)
	require.NoError(t, versioning.SaveHead(db3, "main", sum3))
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
	assertCommitsPersisted(t, db, fs, [][]byte{sum2})
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
	assertCommitsPersisted(t, db, fs, [][]byte{sum1, sum3})
	require.NoError(t, db.Close())
}

func TestFetchCmdCustomRefSpec(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()

	db1 := kv.NewMockStore(false)
	fs1 := kv.NewMockStore(false)
	sum1, _ := packtest.CreateCommit(t, db1, fs1, nil)
	require.NoError(t, versioning.SaveTag(db1, "v1", sum1))
	packtest.RegisterHandler(http.MethodGet, "/info/refs/", pack.NewInfoRefsHandler(db1))
	packtest.RegisterHandler(http.MethodPost, "/upload-pack/", pack.NewUploadPackHandler(db1, fs1))

	rd, cleanUp := createRepoDir(t)
	fs := rd.OpenFileStore()
	defer cleanUp()

	cmd := newRootCmd()
	cmd.SetArgs([]string{"remote", "add", "origin", packtest.TestOrigin})
	require.NoError(t, cmd.Execute())

	cmd = newRootCmd()
	cmd.SetArgs([]string{"fetch", "origin", "refs/tags/*:refs/remotes/origin/tags/*"})
	require.NoError(t, cmd.Execute())
	db, err := rd.OpenKVStore()
	require.NoError(t, err)
	defer db.Close()
	sum, err := versioning.GetRef(db, "remotes/origin/tags/v1")
	require.NoError(t, err)
	assert.Equal(t, sum1, sum)
	assertCommitsPersisted(t, db, fs, [][]byte{sum1})
}
