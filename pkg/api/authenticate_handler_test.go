package api_test

import (
	"bytes"
	"encoding/csv"
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/core/pkg/api/client"
	"github.com/wrgl/core/pkg/api/payload"
	apitest "github.com/wrgl/core/pkg/api/test"
	"github.com/wrgl/core/pkg/auth"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/testutils"
)

func (s *testSuite) TestAuthenticate(t *testing.T) {
	srv := apitest.NewServer(t, regexp.MustCompile(`^/my-repo/`))
	repo, cli, _, cleanup := srv.NewClient(t, false, "/my-repo/", regexp.MustCompile(`^/my-repo`))
	defer cleanup()
	authnS := srv.GetAuthnS(repo)
	authzS := srv.GetAuthzS(repo)
	db := srv.GetDB(repo)
	rs := srv.GetRS(repo)
	sum1, _ := factory.CommitRandom(t, db, nil)
	sum2, com := factory.CommitRandom(t, db, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rs, "main", sum2, com))
	email := "user@test.com"
	require.NoError(t, authnS.SetPassword(email, "password"))

	_, err := cli.Authenticate("not-a-user@test.com", "password")
	assert.Error(t, err)

	_, err = cli.Authenticate(email, "incorrect-password")
	assert.Error(t, err)

	tok, err := cli.Authenticate(email, "password")
	require.NoError(t, err)

	buf := bytes.NewBuffer(nil)
	w := csv.NewWriter(buf)
	require.NoError(t, w.WriteAll(testutils.BuildRawCSV(4, 4)))
	w.Flush()
	_, err = cli.Commit("alpha", "initial commit", "john@doe.com", "John Doe", bytes.NewReader(buf.Bytes()), nil)
	assert.Error(t, err)
	_, err = cli.Commit("alpha", "initial commit", "john@doe.com", "John Doe", bytes.NewReader(buf.Bytes()), nil, apiclient.WithAuthorization(tok))
	assert.Error(t, err)
	_, err = cli.GetCommit(sum2)
	assert.Error(t, err)
	_, err = cli.GetCommit(sum2, apiclient.WithAuthorization(tok))
	assert.Error(t, err)
	_, err = cli.GetTable(com.Table)
	assert.Error(t, err)
	_, err = cli.GetTable(com.Table, apiclient.WithAuthorization(tok))
	assert.Error(t, err)
	_, err = cli.GetBlocks(com.Table, 0, 0, payload.BlockFormatCSV)
	assert.Error(t, err)
	_, err = cli.GetBlocks(com.Table, 0, 0, payload.BlockFormatCSV, apiclient.WithAuthorization(tok))
	assert.Error(t, err)
	_, err = cli.GetRows(com.Table, []int{0})
	assert.Error(t, err)
	_, err = cli.GetRows(com.Table, []int{0}, apiclient.WithAuthorization(tok))
	assert.Error(t, err)
	_, err = cli.Diff(sum1, sum2)
	assert.Error(t, err)
	_, err = cli.Diff(sum1, sum2, apiclient.WithAuthorization(tok))
	assert.Error(t, err)
	_, err = cli.GetRefs()
	assert.Error(t, err)
	_, err = cli.GetRefs(apiclient.WithAuthorization(tok))
	assert.Error(t, err)
	_, _, err = cli.PostUploadPack([][]byte{sum2}, nil, true)
	assert.Error(t, err)
	_, _, err = cli.PostUploadPack([][]byte{sum2}, nil, true, apiclient.WithAuthorization(tok))
	assert.Error(t, err)
	_, err = cli.PostUpdatesToReceivePack(map[string]*payload.Update{"main": {OldSum: payload.BytesToHex(sum2)}})
	assert.Error(t, err)
	_, err = cli.PostUpdatesToReceivePack(map[string]*payload.Update{"main": {OldSum: payload.BytesToHex(sum2)}}, apiclient.WithAuthorization(tok))
	assert.Error(t, err)

	require.NoError(t, authzS.AddPolicy(email, auth.ScopeRead))

	_, err = cli.Commit("alpha", "initial commit", "john@doe.com", "John Doe", bytes.NewReader(buf.Bytes()), nil, apiclient.WithAuthorization(tok))
	assert.Error(t, err)
	gcr, err := cli.GetCommit(sum2, apiclient.WithAuthorization(tok))
	require.NoError(t, err)
	assert.NotEmpty(t, gcr.Table)
	tr, err := cli.GetTable(com.Table, apiclient.WithAuthorization(tok))
	require.NoError(t, err)
	assert.NotEmpty(t, tr.Columns)
	resp, err := cli.GetBlocks(com.Table, 0, 0, payload.BlockFormatCSV, apiclient.WithAuthorization(tok))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp, err = cli.GetRows(com.Table, []int{0}, apiclient.WithAuthorization(tok))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	dr, err := cli.Diff(sum1, sum2, apiclient.WithAuthorization(tok))
	require.NoError(t, err)
	assert.NotEmpty(t, dr.ColDiff)
	refs, err := cli.GetRefs(apiclient.WithAuthorization(tok))
	require.NoError(t, err)
	assert.Greater(t, len(refs), 0)
	_, _, err = cli.PostUploadPack([][]byte{sum2}, nil, true, apiclient.WithAuthorization(tok))
	require.NoError(t, err)
	_, err = cli.PostUpdatesToReceivePack(map[string]*payload.Update{"main": {OldSum: payload.BytesToHex(sum2)}}, apiclient.WithAuthorization(tok))
	assert.Error(t, err)

	require.NoError(t, authzS.AddPolicy(email, auth.ScopeWrite))

	cr, err := cli.Commit("alpha", "initial commit", "john@doe.com", "John Doe", bytes.NewReader(buf.Bytes()), nil, apiclient.WithAuthorization(tok))
	require.NoError(t, err)
	assert.NotEmpty(t, cr.Sum)
	resp, err = cli.PostUpdatesToReceivePack(map[string]*payload.Update{"main": {OldSum: payload.BytesToHex(sum2)}}, apiclient.WithAuthorization(tok))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
