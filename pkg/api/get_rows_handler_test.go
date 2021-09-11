// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"encoding/csv"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
)

func (s *testSuite) TestGetRowsHandler(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, true, "", nil)
	defer cleanup()
	db := s.s.GetDB(repo)

	_, com := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, []uint32{0}, nil)
	tbl, err := objects.GetTable(db, com.Table)
	require.NoError(t, err)

	_, err = cli.GetRows(testutils.SecureRandomBytes(16), nil)
	assertHTTPError(t, err, http.StatusNotFound, "Not Found")

	resp, err := cli.GetRows(com.Table, nil)
	require.NoError(t, err)
	assertBlocksCSV(t, db, tbl.Blocks, resp)

	_, err = cli.GetRows(com.Table, []int{-1})
	assertHTTPError(t, err, http.StatusBadRequest, "offset out of range \"-1\"")

	_, err = cli.GetRows(com.Table, []int{10})
	assertHTTPError(t, err, http.StatusBadRequest, "offset out of range \"10\"")

	resp, err = cli.GetRows(com.Table, []int{0, 1})
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	r := csv.NewReader(resp.Body)
	rows, err := r.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"1", "q", "w"},
		{"2", "a", "s"},
	}, rows)

	resp, err = cli.GetRows(com.Table, []int{2, 1})
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	r = csv.NewReader(resp.Body)
	rows, err = r.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"3", "z", "x"},
		{"2", "a", "s"},
	}, rows)
}
