// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apitest

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/core/pkg/api/client"
	apiutils "github.com/wrgl/core/pkg/api/utils"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/testutils"
)

const (
	TestOrigin = "https://wrgl.test"
)

func RegisterHandler(method, path string, handler http.Handler) {
	RegisterHandlerWithOrigin(TestOrigin, method, path, handler)
}

func decodeGzipPayload(header *http.Header, r io.ReadCloser) (io.ReadCloser, error) {
	if header.Get("Content-Encoding") == "gzip" {
		gzr, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		b, err := ioutil.ReadAll(gzr)
		if err != nil {
			return nil, err
		}
		r = io.NopCloser(bytes.NewReader(b))
		header.Del("Content-Encoding")
	}
	return r, nil
}

func RegisterHandlerWithOrigin(origin, method, path string, handler http.Handler) {
	url := origin + path
	if strings.HasPrefix(path, "=~") {
		url = path
	}
	httpmock.RegisterResponder(method, url,
		func(req *http.Request) (*http.Response, error) {
			rec := httptest.NewRecorder()
			r, err := decodeGzipPayload(&req.Header, req.Body)
			if err != nil {
				return nil, err
			}
			req.Body = r
			handler.ServeHTTP(rec, req)
			resp := rec.Result()
			r, err = decodeGzipPayload(&resp.Header, resp.Body)
			if err != nil {
				return nil, err
			}
			resp.Body = r
			return resp, nil
		},
	)
}

func FetchObjects(t *testing.T, db objects.Store, rs ref.Store, advertised [][]byte, havesPerRoundTrip int) [][]byte {
	t.Helper()
	c, err := apiclient.NewClient(TestOrigin)
	require.NoError(t, err)
	ses, err := apiclient.NewUploadPackSession(db, rs, c, advertised, havesPerRoundTrip)
	require.NoError(t, err)
	commits, err := ses.Start()
	require.NoError(t, err)
	return commits
}

func PushObjects(t *testing.T, db objects.Store, rs ref.Store, updates []*apiutils.Update, remoteRefs map[string][]byte, maxPackfileSize uint64) []*apiutils.Update {
	t.Helper()
	c, err := apiclient.NewClient(TestOrigin)
	require.NoError(t, err)
	ses, err := apiclient.NewReceivePackSession(db, rs, c, updates, remoteRefs, maxPackfileSize)
	require.NoError(t, err)
	updates, err = ses.Start()
	require.NoError(t, err)
	return updates
}

func CopyCommitsToNewStore(t *testing.T, src, dst objects.Store, commits [][]byte) {
	t.Helper()
	enc := objects.NewStrListEncoder(true)
	for _, sum := range commits {
		c, err := objects.GetCommit(src, sum)
		require.NoError(t, err)
		tbl, err := objects.GetTable(src, c.Table)
		require.NoError(t, err)
		buf := bytes.NewBuffer(nil)
		for _, sum := range tbl.Blocks {
			blk, err := objects.GetBlock(src, sum)
			require.NoError(t, err)
			buf.Reset()
			_, err = objects.WriteBlockTo(enc, buf, blk)
			require.NoError(t, err)
			_, err = objects.SaveBlock(dst, buf.Bytes())
			require.NoError(t, err)
		}
		buf.Reset()
		_, err = tbl.WriteTo(buf)
		require.NoError(t, err)
		_, err = objects.SaveTable(dst, buf.Bytes())
		require.NoError(t, err)
		buf.Reset()
		_, err = c.WriteTo(buf)
		require.NoError(t, err)
		_, err = objects.SaveCommit(dst, buf.Bytes())
		require.NoError(t, err)
		require.NoError(t, ingest.IndexTable(dst, c.Table, tbl))
	}
}

func AssertCommitsPersisted(t *testing.T, db objects.Store, commits [][]byte) {
	t.Helper()
	for _, sum := range commits {
		c, err := objects.GetCommit(db, sum)
		require.NoError(t, err, "commit %x not found", sum)
		tbl, err := objects.GetTable(db, c.Table)
		require.NoError(t, err, "table %x not found", c.Table)
		for _, blk := range tbl.Blocks {
			assert.True(t, objects.BlockExist(db, blk), "block %x not found", blk)
		}
	}
}

func ReceivePackConfig(denyNonFastForwards, denyDeletes bool) *conf.Config {
	return &conf.Config{
		User: &conf.ConfigUser{
			Name:  "test",
			Email: "test@domain.com",
		},
		Receive: &conf.ConfigReceive{
			DenyNonFastForwards: &denyNonFastForwards,
			DenyDeletes:         &denyDeletes,
		},
	}
}

func CreateRandomCommit(t *testing.T, db objects.Store, numCols, numRows int, parents [][]byte) ([]byte, *objects.Commit) {
	t.Helper()
	rows := testutils.BuildRawCSV(numCols, numRows)
	buf := bytes.NewBuffer(nil)
	w := csv.NewWriter(buf)
	require.NoError(t, w.WriteAll(rows))
	sum, err := ingest.IngestTable(db, io.NopCloser(bytes.NewReader(buf.Bytes())), rows[0][:1], 0, 1, io.Discard)
	require.NoError(t, err)
	com := &objects.Commit{
		Table:       sum,
		Parents:     parents,
		Time:        time.Now(),
		AuthorName:  testutils.BrokenRandomLowerAlphaString(10),
		AuthorEmail: testutils.BrokenRandomLowerAlphaString(10),
		Message:     testutils.BrokenRandomAlphaNumericString(10),
	}
	buf.Reset()
	_, err = com.WriteTo(buf)
	require.NoError(t, err)
	sum, err = objects.SaveCommit(db, buf.Bytes())
	require.NoError(t, err)
	return sum, com
}

func PostMultipartForm(t *testing.T, path string, value map[string][]string, files map[string]io.Reader) *http.Response {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	w := multipart.NewWriter(buf)
	for k, sl := range value {
		for _, v := range sl {
			require.NoError(t, w.WriteField(k, v))
		}
	}
	for k, r := range files {
		w, err := w.CreateFormFile(k, k)
		require.NoError(t, err)
		io.Copy(w, r)
	}
	require.NoError(t, w.Close())
	req, err := http.NewRequest(http.MethodPost, TestOrigin+path, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func Get(t *testing.T, path string) *http.Response {
	t.Helper()
	resp, err := http.Get(TestOrigin + path)
	require.NoError(t, err)
	return resp
}
