// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apitest

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"io"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/conf"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/sorter"
	"github.com/wrgl/wrgl/pkg/testutils"
)

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
		gzr.Close()
		if err := r.Close(); err != nil {
			return nil, err
		}
		r = io.NopCloser(bytes.NewReader(b))
		header.Del("Content-Encoding")
	}
	return r, nil
}

type GZIPAwareHandler struct {
	T           *testing.T
	Handler     http.Handler
	HandlerFunc http.HandlerFunc
}

func (h *GZIPAwareHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	reader, err := decodeGzipPayload(&r.Header, r.Body)
	require.NoError(h.T, err)
	r.Body = reader
	if h.HandlerFunc != nil {
		h.HandlerFunc(rw, r)
		return
	}
	h.Handler.ServeHTTP(rw, r)
}

func FetchObjects(t *testing.T, db objects.Store, rs ref.Store, c *apiclient.Client, advertised [][]byte, havesPerRoundTrip, depth int, opts ...apiclient.RequestOption) [][]byte {
	t.Helper()
	ses, err := apiclient.NewUploadPackSession(db, rs, c, advertised, havesPerRoundTrip, depth, opts...)
	require.NoError(t, err)
	commits, err := ses.Start(nil)
	require.NoError(t, err)
	return commits
}

func PushObjects(t *testing.T, db objects.Store, rs ref.Store, c *apiclient.Client, updates map[string]*payload.Update, remoteRefs map[string][]byte, maxPackfileSize uint64, opts ...apiclient.RequestOption) map[string]*payload.Update {
	t.Helper()
	ses, err := apiclient.NewReceivePackSession(db, rs, c, updates, remoteRefs, maxPackfileSize, opts...)
	require.NoError(t, err)
	updates, err = ses.Start()
	require.NoError(t, err)
	return updates
}

func CopyCommitsToNewStore(t *testing.T, src, dst objects.Store, commits [][]byte) {
	t.Helper()
	enc := objects.NewStrListEncoder(true)
	buf := bytes.NewBuffer(nil)
	var bb []byte
	var blk [][]string
	for _, sum := range commits {
		c, err := objects.GetCommit(src, sum)
		require.NoError(t, err)
		tbl, err := objects.GetTable(src, c.Table)
		require.NoError(t, err)
		for _, sum := range tbl.Blocks {
			blk, bb, err = objects.GetBlock(src, bb, sum)
			require.NoError(t, err)
			buf.Reset()
			_, err = objects.WriteBlockTo(enc, buf, blk)
			require.NoError(t, err)
			_, bb, err = objects.SaveBlock(dst, bb, buf.Bytes())
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
		require.NoError(t, ingest.IndexTable(dst, c.Table, tbl, nil))
		require.NoError(t, ingest.ProfileTable(dst, c.Table, tbl))
	}
}

func AssertTableNotPersisted(t *testing.T, db objects.Store, table []byte) {
	t.Helper()
	assert.False(t, objects.TableExist(db, table))
}

func AssertTablePersisted(t *testing.T, db objects.Store, table []byte) {
	t.Helper()
	tbl, err := objects.GetTable(db, table)
	require.NoError(t, err, "table %x not found", table)
	for _, blk := range tbl.Blocks {
		assert.True(t, objects.BlockExist(db, blk), "block %x not found", blk)
	}
	_, err = objects.GetTableIndex(db, table)
	require.NoError(t, err)
	_, err = objects.GetTableProfile(db, table)
	require.NoError(t, err)
}

func AssertTablesNotPersisted(t *testing.T, db objects.Store, tables [][]byte) {
	t.Helper()
	for _, sum := range tables {
		AssertTableNotPersisted(t, db, sum)
	}
}

func AssertTablesPersisted(t *testing.T, db objects.Store, tables [][]byte) {
	t.Helper()
	for _, sum := range tables {
		AssertTablePersisted(t, db, sum)
	}
}

func AssertCommitsPersisted(t *testing.T, db objects.Store, commits [][]byte) {
	t.Helper()
	for _, sum := range commits {
		c, err := objects.GetCommit(db, sum)
		require.NoError(t, err, "commit %x not found", sum)
		AssertTablePersisted(t, db, c.Table)
	}
}

func AssertCommitsShallowlyPersisted(t *testing.T, db objects.Store, commits [][]byte) {
	t.Helper()
	for _, sum := range commits {
		assert.True(t, objects.CommitExist(db, sum))
	}
}

func ReceivePackConfig(denyNonFastForwards, denyDeletes bool) *conf.Config {
	return &conf.Config{
		User: &conf.User{
			Name:  "test",
			Email: "test@domain.com",
		},
		Receive: &conf.Receive{
			DenyNonFastForwards: &denyNonFastForwards,
			DenyDeletes:         &denyDeletes,
		},
	}
}

func CreateRandomCommitWithTable(t *testing.T, db objects.Store, tableSum []byte, parents [][]byte) ([]byte, *objects.Commit) {
	t.Helper()
	com := &objects.Commit{
		Table:       tableSum,
		Parents:     parents,
		Time:        time.Now(),
		AuthorName:  testutils.BrokenRandomLowerAlphaString(10),
		AuthorEmail: testutils.BrokenRandomLowerAlphaString(10),
		Message:     testutils.BrokenRandomAlphaNumericString(10),
	}
	buf := bytes.NewBuffer(nil)
	_, err := com.WriteTo(buf)
	require.NoError(t, err)
	sum, err := objects.SaveCommit(db, buf.Bytes())
	require.NoError(t, err)
	return sum, com
}

func CreateRandomCommit(t *testing.T, db objects.Store, numCols, numRows int, parents [][]byte) ([]byte, *objects.Commit) {
	t.Helper()
	rows := testutils.BuildRawCSV(numCols, numRows)
	buf := bytes.NewBuffer(nil)
	w := csv.NewWriter(buf)
	require.NoError(t, w.WriteAll(rows))
	s, err := sorter.NewSorter(0, nil)
	require.NoError(t, err)
	sum, err := ingest.IngestTable(db, s, io.NopCloser(bytes.NewReader(buf.Bytes())), rows[0][:1])
	require.NoError(t, err)
	return CreateRandomCommitWithTable(t, db, sum, parents)
}
