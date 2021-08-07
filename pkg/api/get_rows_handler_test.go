// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"encoding/csv"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/core/pkg/api/client"
	apiserver "github.com/wrgl/core/pkg/api/server"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/router"
	"github.com/wrgl/core/pkg/testutils"
)

func TestGetRowsHandler(t *testing.T) {
	db := objmock.NewStore()
	ts := httptest.NewServer(router.NewRouter(&router.Routes{
		Pat: regexp.MustCompile(`^/tables/[0-9a-f]{32}/`),
		Subs: []*router.Routes{
			{
				Method:  http.MethodGet,
				Pat:     regexp.MustCompile(`^blocks/`),
				Handler: apiserver.NewGetBlocksHandler(db),
			},
			{
				Method:  http.MethodGet,
				Pat:     regexp.MustCompile(`^rows/`),
				Handler: apiserver.NewGetRowsHandler(db),
			},
		},
	}))
	defer ts.Close()
	cli, err := apiclient.NewClient(ts.URL)
	require.NoError(t, err)

	_, com := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, []uint32{0}, nil)
	tbl, err := objects.GetTable(db, com.Table)
	require.NoError(t, err)

	_, err = cli.GetRows(testutils.SecureRandomBytes(16), nil)
	assert.Equal(t, "status 404: 404 page not found", err.Error())

	resp, err := cli.GetRows(com.Table, nil)
	require.NoError(t, err)
	assertBlocksCSV(t, db, tbl.Blocks, resp)

	_, err = cli.GetRows(com.Table, []int{-1})
	assert.Equal(t, "status 400: offset out of range \"-1\"", err.Error())

	_, err = cli.GetRows(com.Table, []int{10})
	assert.Equal(t, "status 400: offset out of range \"10\"", err.Error())

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
