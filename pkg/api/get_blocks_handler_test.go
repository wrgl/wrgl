// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"encoding/csv"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/api"
	apiclient "github.com/wrgl/core/pkg/api/client"
	"github.com/wrgl/core/pkg/api/payload"
	apitest "github.com/wrgl/core/pkg/api/test"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/testutils"
)

func assertBlocksCSV(t *testing.T, db objects.Store, blocks [][]byte, resp *http.Response) {
	t.Helper()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, api.CTCSV, resp.Header.Get("Content-Type"))
	defer resp.Body.Close()
	r := csv.NewReader(resp.Body)
	for i, sum := range blocks {
		blk, err := objects.GetBlock(db, sum)
		require.NoError(t, err)
		for j, row := range blk {
			sl, err := r.Read()
			require.NoError(t, err)
			require.Equal(t, row, sl, "row (%d, %d)", i, j)
		}
	}
	_, err := r.Read()
	assert.Equal(t, io.EOF, err)
}

func assertBlocksBinary(t *testing.T, db objects.Store, blocks [][]byte, resp *http.Response) {
	t.Helper()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, api.CTBlocksBinary, resp.Header.Get("Content-Type"))
	defer resp.Body.Close()
	for i, sum := range blocks {
		blk1, err := objects.GetBlock(db, sum)
		require.NoError(t, err)
		_, blk2, err := objects.ReadBlockFrom(resp.Body)
		require.NoError(t, err)
		require.Equal(t, blk1, blk2, "block %d", i)
	}
	_, _, err := objects.ReadBlockFrom(resp.Body)
	assert.Equal(t, io.EOF, err)
}

func (s *testSuite) TestGetBlocksHandler(t *testing.T) {
	repo, cli, m, cleanup := s.NewClient(t)
	defer cleanup()
	db := s.getDB(repo)

	_, com := apitest.CreateRandomCommit(t, db, 5, 700, nil)
	tbl, err := objects.GetTable(db, com.Table)
	require.NoError(t, err)

	_, err = cli.GetBlocks(testutils.SecureRandomBytes(16), 0, 0, "")
	assert.Equal(t, "status 404: 404 page not found", err.Error())

	_, err = cli.GetBlocks(com.Table, 7, 0, "")
	assert.Equal(t, "status 400: start out of range", err.Error())

	_, err = cli.GetBlocks(com.Table, 0, 7, "")
	assert.Equal(t, "status 400: end out of range", err.Error())

	resp, err := cli.GetBlocks(com.Table, 0, 0, "")
	require.NoError(t, err)
	assertBlocksCSV(t, db, tbl.Blocks, resp)

	resp, err = cli.GetBlocks(com.Table, 0, 1, "")
	require.NoError(t, err)
	assertBlocksCSV(t, db, tbl.Blocks[:1], resp)

	resp, err = cli.GetBlocks(com.Table, 1, 2, "")
	require.NoError(t, err)
	assertBlocksCSV(t, db, tbl.Blocks[1:2], resp)

	_, err = cli.GetBlocks(com.Table, 0, 0, "abc")
	assert.Equal(t, "status 400: invalid format", err.Error())

	resp, err = cli.GetBlocks(com.Table, 0, 0, payload.BlockFormatBinary)
	require.NoError(t, err)
	assertBlocksBinary(t, db, tbl.Blocks, resp)

	// pass custom header
	req := m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "4567")
		resp, err = cli.GetBlocks(com.Table, 0, 0, "", apiclient.WithHeader(header))
		require.NoError(t, err)
		assertBlocksCSV(t, db, tbl.Blocks, resp)
	})
	assert.Equal(t, "4567", req.Header.Get("Custom-Header"))
}
