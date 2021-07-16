// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/api"
	"github.com/wrgl/core/pkg/api/payload"
	apitest "github.com/wrgl/core/pkg/api/test"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/testutils"
)

func TestGetCommitHandler(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := objmock.NewStore()
	parent, _ := factory.CommitRandom(t, db, nil)
	sum, com := factory.CommitRandom(t, db, [][]byte{parent})
	apitest.RegisterHandler(http.MethodGet, `=~^/commits/[0-9a-f]+/\z`, api.NewGetCommitHandler(db))
	apitest.RegisterHandler(http.MethodGet, `=~^/tables/[0-9a-f]+/\z`, api.NewGetTableHandler(db))

	// get commit not found
	resp := apitest.Get(t, fmt.Sprintf("/commits/%x/", testutils.SecureRandomBytes(16)))
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// get commit OK
	resp = apitest.Get(t, fmt.Sprintf("/commits/%x/", sum))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	cr := &payload.GetCommitResponse{}
	require.NoError(t, json.Unmarshal(b, cr))
	assert.Equal(t, com.Table, (*cr.Table)[:])
	assert.Equal(t, com.AuthorName, cr.AuthorName)
	assert.Equal(t, com.AuthorEmail, cr.AuthorEmail)
	assert.Equal(t, com.Message, cr.Message)
	assert.Equal(t, com.Time.Format(time.RFC3339), cr.Time.Format(time.RFC3339))
	assert.Len(t, cr.Parents, 1)
	assert.Equal(t, com.Parents[0], (*cr.Parents[0])[:])

	// get table not found
	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/", testutils.SecureRandomBytes(16)))
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// get table OK
	resp = apitest.Get(t, fmt.Sprintf("/tables/%x/", com.Table))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	b, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	tr := &payload.GetTableResponse{}
	require.NoError(t, json.Unmarshal(b, tr))
	tbl, err := objects.GetTable(db, com.Table)
	require.NoError(t, err)
	assert.Equal(t, tbl.Columns, tr.Columns)
	assert.Equal(t, tbl.PK, tr.PK)
	assert.Equal(t, tbl.RowsCount, tr.RowsCount)
}
