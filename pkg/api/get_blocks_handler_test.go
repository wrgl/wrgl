// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/api"
	apitest "github.com/wrgl/core/pkg/api/test"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
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

func TestGetBlocksHandler(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := objmock.NewStore()
	apitest.RegisterHandler(http.MethodGet, `=~^/tables/[0-9a-f]+/blocks/\z`, api.NewGetBlocksHandler(db))

	_, com := apitest.CreateRandomCommit(t, db, 5, 700, nil)
	tbl, err := objects.GetTable(db, com.Table)
	require.NoError(t, err)

	resp := apitest.Get(t, fmt.Sprintf("/tables/%x/blocks/", testutils.SecureRandomBytes(16)))
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/blocks/?start=abc", com.Table))
	apitest.AssertResp(t, resp, http.StatusBadRequest, "invalid start\n")

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/blocks/?start=7", com.Table))
	apitest.AssertResp(t, resp, http.StatusBadRequest, "start out of range\n")

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/blocks/?end=abc", com.Table))
	apitest.AssertResp(t, resp, http.StatusBadRequest, "invalid end\n")

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/blocks/?end=7", com.Table))
	apitest.AssertResp(t, resp, http.StatusBadRequest, "end out of range\n")

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/blocks/", com.Table))
	assertBlocksCSV(t, db, tbl.Blocks, resp)

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/blocks/?end=1", com.Table))
	assertBlocksCSV(t, db, tbl.Blocks[:1], resp)

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/blocks/?start=1&end=2", com.Table))
	assertBlocksCSV(t, db, tbl.Blocks[1:2], resp)

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/blocks/?format=abc", com.Table))
	apitest.AssertResp(t, resp, http.StatusBadRequest, "invalid format\n")

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/blocks/?format=binary", com.Table))
	assertBlocksBinary(t, db, tbl.Blocks, resp)
}
