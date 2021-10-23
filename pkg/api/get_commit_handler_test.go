// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func (s *testSuite) TestGetCommitHandler(t *testing.T) {
	repo, cli, m, cleanup := s.s.NewClient(t, true, "", nil)
	defer cleanup()
	db := s.s.GetDB(repo)
	parent, _ := factory.CommitRandom(t, db, nil)
	sum, com := factory.CommitRandom(t, db, [][]byte{parent})
	tbl, err := objects.GetTable(db, com.Table)
	require.NoError(t, err)

	// get commit not found
	_, err = cli.GetCommit(testutils.SecureRandomBytes(16))
	assertHTTPError(t, err, http.StatusNotFound, "Not Found")

	// get commit OK
	cr, err := cli.GetCommit(sum)
	require.NoError(t, err)
	assert.Equal(t, &payload.Table{
		Sum:       payload.BytesToHex(com.Table),
		Columns:   tbl.Columns,
		RowsCount: tbl.RowsCount,
		PK:        tbl.PK,
	}, cr.Table)
	assert.Equal(t, sum, cr.Sum[:])
	assert.Equal(t, com.AuthorName, cr.AuthorName)
	assert.Equal(t, com.AuthorEmail, cr.AuthorEmail)
	assert.Equal(t, com.Message, cr.Message)
	assert.Equal(t, com.Time.Format(time.RFC3339), cr.Time.Format(time.RFC3339))
	assert.Len(t, cr.Parents, 1)
	assert.Equal(t, com.Parents[0], (*cr.Parents[0])[:])

	// get table not found
	_, err = cli.GetTable(testutils.SecureRandomBytes(16))
	assertHTTPError(t, err, http.StatusNotFound, "Not Found")

	// get table OK
	tr, err := cli.GetTable(com.Table)
	require.NoError(t, err)
	assert.Equal(t, tbl.Columns, tr.Columns)
	assert.Equal(t, tbl.PK, tr.PK)
	assert.Equal(t, tbl.RowsCount, tr.RowsCount)

	// pass custom header
	req := m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "123")
		cr, err = cli.GetCommit(sum, apiclient.WithRequestHeader(header))
		require.NoError(t, err)
		assert.NotEmpty(t, cr)
	})
	assert.Equal(t, "123", req.Header.Get("Custom-Header"))
	req = m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "456")
		tr, err = cli.GetTable(com.Table, apiclient.WithRequestHeader(header))
		require.NoError(t, err)
		assert.NotEmpty(t, tr)
	})
	assert.Equal(t, "456", req.Header.Get("Custom-Header"))
}
