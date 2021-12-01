// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/api"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/api/payload"
	apitest "github.com/wrgl/wrgl/pkg/api/test"
	"github.com/wrgl/wrgl/pkg/encoding"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func assertBlocksCSV(t *testing.T, db objects.Store, blocks [][]byte, columns []string, resp *http.Response) {
	t.Helper()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, api.CTCSV, resp.Header.Get("Content-Type"))
	defer resp.Body.Close()
	r := csv.NewReader(resp.Body)
	if columns != nil {
		row, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, columns, row)
	}
	var bb []byte
	var blk [][]string
	var err error
	for i, sum := range blocks {
		blk, bb, err = objects.GetBlock(db, bb, sum)
		require.NoError(t, err)
		for j, row := range blk {
			sl, err := r.Read()
			require.NoError(t, err)
			require.Equal(t, row, sl, "row (%d, %d)", i, j)
		}
	}
	_, err = r.Read()
	assert.Equal(t, io.EOF, err)
}

func assertBlocksBinary(t *testing.T, db objects.Store, blocks [][]byte, resp *http.Response) {
	t.Helper()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, api.CTPackfile, resp.Header.Get("Content-Type"))
	defer resp.Body.Close()
	var bb []byte
	var blk1 [][]string
	pr, err := encoding.NewPackfileReader(resp.Body)
	require.NoError(t, err)
	for i, sum := range blocks {
		blk1, bb, err = objects.GetBlock(db, bb, sum)
		require.NoError(t, err)
		ot, b, err := pr.ReadObject()
		require.NoError(t, err)
		assert.Equal(t, encoding.ObjectBlock, ot)
		bb, err = s2.Decode(bb, b)
		require.NoError(t, err)
		_, blk2, err := objects.ReadBlockFrom(bytes.NewReader(bb))
		require.NoError(t, err)
		require.Equal(t, blk1, blk2, "block %d", i)
	}
	_, _, err = pr.ReadObject()
	assert.Equal(t, io.EOF, err)
	require.NoError(t, pr.Close())
}

func (s *testSuite) TestGetBlocksHandler(t *testing.T) {
	repo, cli, m, cleanup := s.s.NewClient(t, true, "", nil)
	defer cleanup()
	db := s.s.GetDB(repo)

	sum, com := apitest.CreateRandomCommit(t, db, 5, 700, nil)
	tbl, err := objects.GetTable(db, com.Table)
	require.NoError(t, err)

	_, err = cli.GetTableBlocks(testutils.SecureRandomBytes(16), 0, 0, "", false)
	assertHTTPError(t, err, http.StatusNotFound, "Not Found")

	_, err = cli.GetTableBlocks(com.Table, 7, 0, "", false)
	assertHTTPError(t, err, http.StatusBadRequest, "start out of range")

	_, err = cli.GetTableBlocks(com.Table, 0, 7, "", false)
	assertHTTPError(t, err, http.StatusBadRequest, "end out of range")

	resp, err := cli.GetTableBlocks(com.Table, 0, 0, "", false)
	require.NoError(t, err)
	assertBlocksCSV(t, db, tbl.Blocks, nil, resp)

	resp, err = cli.GetTableBlocks(com.Table, 0, 1, "", false)
	require.NoError(t, err)
	assertBlocksCSV(t, db, tbl.Blocks[:1], nil, resp)

	resp, err = cli.GetTableBlocks(com.Table, 1, 2, "", false)
	require.NoError(t, err)
	assertBlocksCSV(t, db, tbl.Blocks[1:2], nil, resp)

	resp, err = cli.GetTableBlocks(com.Table, 0, 0, "", true)
	require.NoError(t, err)
	assertBlocksCSV(t, db, tbl.Blocks, tbl.Columns, resp)

	_, err = cli.GetTableBlocks(com.Table, 0, 0, "abc", false)
	assertHTTPError(t, err, http.StatusBadRequest, "invalid format")

	resp, err = cli.GetTableBlocks(com.Table, 0, 0, payload.BlockFormatBinary, false)
	require.NoError(t, err)
	assertBlocksBinary(t, db, tbl.Blocks, resp)

	_, err = cli.GetBlocks(hex.EncodeToString(testutils.SecureRandomBytes(16)), 0, 0, "", false)
	assertHTTPError(t, err, http.StatusNotFound, "Not Found")

	resp, err = cli.GetBlocks(hex.EncodeToString(sum), 0, 0, "", false)
	require.NoError(t, err)
	assertBlocksCSV(t, db, tbl.Blocks, nil, resp)

	// pass custom header
	req := m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "4567")
		resp, err = cli.GetTableBlocks(com.Table, 0, 0, "", false, apiclient.WithRequestHeader(header))
		require.NoError(t, err)
		assertBlocksCSV(t, db, tbl.Blocks, nil, resp)
	})
	assert.Equal(t, "4567", req.Header.Get("Custom-Header"))

	req = m.Capture(t, func(header http.Header) {
		header.Set("Custom-Header", "2345")
		resp, err = cli.GetBlocks(hex.EncodeToString(sum), 0, 0, "", false, apiclient.WithRequestHeader(header))
		require.NoError(t, err)
		assertBlocksCSV(t, db, tbl.Blocks, nil, resp)
	})
	assert.Equal(t, "2345", req.Header.Get("Custom-Header"))
}

func (s *testSuite) TestCookieAuthentication(t *testing.T) {
	repo, cli, _, cleanup := s.s.NewClient(t, false, "", nil)
	defer cleanup()
	s.s.AddUser(t, repo)
	tok := s.s.GetToken(t, repo)
	db := s.s.GetDB(repo)
	_, com := apitest.CreateRandomCommit(t, db, 5, 700, nil)

	// no authentication mechanism
	_, err := cli.GetTableBlocks(com.Table, 0, 0, "", false)
	assertHTTPError(t, err, http.StatusUnauthorized, "unauthorized")

	// authenticate via cookie
	opt := apiclient.WithRequestCookies([]*http.Cookie{
		{
			Name:  "Authorization",
			Value: fmt.Sprintf("Bearer %s", tok),
		},
	})
	_, err = cli.GetTableBlocks(com.Table, 0, 0, "", false, opt)
	require.NoError(t, err)

	// authenticate with url-encoded token
	_, err = cli.GetTableBlocks(com.Table, 0, 0, "", false, apiclient.WithRequestCookies([]*http.Cookie{
		{
			Name:  "Authorization",
			Value: url.PathEscape(fmt.Sprintf("Bearer %s", tok)),
		},
	}))
	require.NoError(t, err)
	_, err = cli.GetTableBlocks(com.Table, 0, 0, "", false, apiclient.WithRequestCookies([]*http.Cookie{
		{
			Name:  "Authorization",
			Value: url.QueryEscape(fmt.Sprintf("Bearer %s", tok)),
		},
	}))
	require.NoError(t, err)

	// authenticate via cookie doesn't work for methods other than GET
	buf := bytes.NewBuffer(nil)
	w := csv.NewWriter(buf)
	require.NoError(t, w.WriteAll(testutils.BuildRawCSV(4, 4)))
	w.Flush()
	_, err = cli.Commit("alpha", "initial commit", "file.csv", bytes.NewReader(buf.Bytes()), nil, opt)
	assertHTTPError(t, err, http.StatusUnauthorized, "unauthorized")
}
