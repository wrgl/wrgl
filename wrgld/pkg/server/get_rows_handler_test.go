package server_test

import (
	"encoding/csv"
	"encoding/hex"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func (s *testSuite) TestGetRowsHandler(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, "", nil, true)
	defer cleanup()
	db := s.s.GetDB(repo)

	sum, com := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, []uint32{0}, nil)
	tbl, err := objects.GetTable(db, com.Table)
	require.NoError(t, err)

	_, err = cli.GetTableRows(testutils.SecureRandomBytes(16), nil)
	assertHTTPError(t, err, http.StatusNotFound, "Not Found")

	resp, err := cli.GetTableRows(com.Table, nil)
	require.NoError(t, err)
	assertBlocksCSV(t, db, tbl.Blocks, nil, resp)

	_, err = cli.GetTableRows(com.Table, []int{-1})
	assertHTTPError(t, err, http.StatusBadRequest, "offset out of range \"-1\"")

	_, err = cli.GetTableRows(com.Table, []int{10})
	assertHTTPError(t, err, http.StatusBadRequest, "offset out of range \"10\"")

	resp, err = cli.GetTableRows(com.Table, []int{0, 1})
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	r := csv.NewReader(resp.Body)
	rows, err := r.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"1", "q", "w"},
		{"2", "a", "s"},
	}, rows)

	resp, err = cli.GetTableRows(com.Table, []int{2, 1})
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	r = csv.NewReader(resp.Body)
	rows, err = r.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"3", "z", "x"},
		{"2", "a", "s"},
	}, rows)

	_, err = cli.GetRows(hex.EncodeToString(testutils.SecureRandomBytes(16)), nil)
	assertHTTPError(t, err, http.StatusNotFound, "Not Found")

	resp, err = cli.GetRows(hex.EncodeToString(sum), nil)
	require.NoError(t, err)
	assertBlocksCSV(t, db, tbl.Blocks, nil, resp)
}
