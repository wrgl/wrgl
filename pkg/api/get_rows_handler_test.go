// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/api"
	apitest "github.com/wrgl/core/pkg/api/test"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/testutils"
)

func TestGetRowsHandler(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := objmock.NewStore()
	apitest.RegisterHandler(http.MethodGet, `=~^/tables/[0-9a-f]+/rows/\z`, api.NewGetRowsHandler(db))
	apitest.RegisterHandler(http.MethodGet, `=~^/tables/[0-9a-f]+/blocks/\z`, api.NewGetBlocksHandler(db))

	_, com := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, []uint32{0}, nil)
	tbl, err := objects.GetTable(db, com.Table)
	require.NoError(t, err)

	resp := apitest.Get(t, fmt.Sprintf("/tables/%x/rows/", testutils.SecureRandomBytes(16)))
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/rows/", com.Table))
	assertBlocksCSV(t, db, tbl.Blocks, resp)

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/rows/?offsets=abc", com.Table))
	apitest.AssertResp(t, resp, http.StatusBadRequest, "invalid offset \"abc\"\n")

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/rows/?offsets=-1", com.Table))
	apitest.AssertResp(t, resp, http.StatusBadRequest, "offset out of range \"-1\"\n")

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/rows/?offsets=10", com.Table))
	apitest.AssertResp(t, resp, http.StatusBadRequest, "offset out of range \"10\"\n")

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/rows/?offsets=0,1", com.Table))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	r := csv.NewReader(resp.Body)
	rows, err := r.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"1", "q", "w"},
		{"2", "a", "s"},
	}, rows)

	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/rows/?offsets=2,1", com.Table))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	r = csv.NewReader(resp.Body)
	rows, err = r.ReadAll()
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"3", "z", "x"},
		{"2", "a", "s"},
	}, rows)
}
