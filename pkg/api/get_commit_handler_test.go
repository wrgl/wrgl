// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/core/pkg/api/client"
	apiserver "github.com/wrgl/core/pkg/api/server"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/testutils"
)

func TestGetCommitHandler(t *testing.T) {
	db := objmock.NewStore()
	parent, _ := factory.CommitRandom(t, db, nil)
	sum, com := factory.CommitRandom(t, db, [][]byte{parent})
	sm := http.NewServeMux()
	sm.Handle("/commits/", apiserver.NewGetCommitHandler(db))
	sm.Handle("/tables/", apiserver.NewGetTableHandler(db))
	cli, m, cleanup := createClient(t, sm)
	defer cleanup()

	// get commit not found
	_, err := cli.GetCommit(testutils.SecureRandomBytes(16))
	assert.Equal(t, "status 404: 404 page not found", err.Error())

	// get commit OK
	cr, err := cli.GetCommit(sum)
	require.NoError(t, err)
	assert.Equal(t, com.Table, (*cr.Table)[:])
	assert.Equal(t, com.AuthorName, cr.AuthorName)
	assert.Equal(t, com.AuthorEmail, cr.AuthorEmail)
	assert.Equal(t, com.Message, cr.Message)
	assert.Equal(t, com.Time.Format(time.RFC3339), cr.Time.Format(time.RFC3339))
	assert.Len(t, cr.Parents, 1)
	assert.Equal(t, com.Parents[0], (*cr.Parents[0])[:])

	// get table not found
	_, err = cli.GetTable(testutils.SecureRandomBytes(16))
	assert.Equal(t, "status 404: 404 page not found", err.Error())

	// get table OK
	tr, err := cli.GetTable(com.Table)
	require.NoError(t, err)
	tbl, err := objects.GetTable(db, com.Table)
	require.NoError(t, err)
	assert.Equal(t, tbl.Columns, tr.Columns)
	assert.Equal(t, tbl.PK, tr.PK)
	assert.Equal(t, tbl.RowsCount, tr.RowsCount)

	// pass custom header
	req := m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "123")
		cr, err = cli.GetCommit(sum, apiclient.WithHeader(header))
		require.NoError(t, err)
		assert.NotEmpty(t, cr)
	})
	assert.Equal(t, "123", req.Header.Get("Custom-Header"))
	req = m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "456")
		tr, err = cli.GetTable(com.Table, apiclient.WithHeader(header))
		require.NoError(t, err)
		assert.NotEmpty(t, tr)
	})
	assert.Equal(t, "456", req.Header.Get("Custom-Header"))
}
