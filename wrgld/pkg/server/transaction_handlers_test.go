package server_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func (s *testSuite) TestTransaction(t *testing.T) {
	_, cli, _, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()

	ctr, err := cli.CreateTransaction()
	require.NoError(t, err)
	tid, err := uuid.Parse(ctr.ID)
	require.NoError(t, err)

	cr1, err := cli.Commit("alpha", "initial commit", "file.csv", testutils.RawCSVBytesReader(testutils.BuildRawCSV(3, 4)), nil, nil)
	require.NoError(t, err)
	cr2, err := cli.Commit("alpha", "second commit", "file.csv", testutils.RawCSVBytesReader(testutils.BuildRawCSV(3, 4)), nil, &tid)
	require.NoError(t, err)
	cr3, err := cli.Commit("beta", "initial commit", "file.csv", testutils.RawCSVBytesReader(testutils.BuildRawCSV(3, 4)), nil, &tid)
	require.NoError(t, err)

	gtr, err := cli.GetTransaction(tid)
	require.NoError(t, err)
	assert.NotEmpty(t, gtr.Begin)
	assert.Equal(t, []payload.TxBranch{
		{
			Name:       "alpha",
			CurrentSum: cr1.Sum.String(),
			NewSum:     cr2.Sum.String(),
		},
		{
			Name:   "beta",
			NewSum: cr3.Sum.String(),
		},
	}, gtr.Branches)

	resp, err := cli.DiscardTransaction(tid)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	_, err = cli.GetTransaction(tid)
	assert.Error(t, err)

	ctr, err = cli.CreateTransaction()
	require.NoError(t, err)
	tid, err = uuid.Parse(ctr.ID)
	require.NoError(t, err)
	cr4, err := cli.Commit("alpha", "second commit", "file.csv", testutils.RawCSVBytesReader(testutils.BuildRawCSV(3, 4)), nil, &tid)
	require.NoError(t, err)
	cr5, err := cli.Commit("beta", "initial commit", "file.csv", testutils.RawCSVBytesReader(testutils.BuildRawCSV(3, 4)), nil, &tid)
	require.NoError(t, err)

	resp, err = cli.CommitTransaction(tid)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	_, err = cli.GetTransaction(tid)
	assert.Error(t, err)

	com1, err := cli.GetHead("alpha")
	require.NoError(t, err)
	assert.Equal(t, []*payload.Hex{
		cr1.Sum,
	}, com1.Parents)
	assert.Equal(t, cr4.Table, com1.Table.Sum)

	com2, err := cli.GetHead("beta")
	require.NoError(t, err)
	assert.Len(t, com2.Parents, 0)
	assert.Equal(t, cr5.Table, com2.Table.Sum)
}
