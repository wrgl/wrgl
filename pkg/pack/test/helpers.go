// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package packtest

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/mmcloughlin/meow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/kv"
	packclient "github.com/wrgl/core/pkg/pack/client"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/pkg/versioning"
)

const (
	TestOrigin = "https://wrgl.test"
)

var tg func() time.Time

func init() {
	tg = testutils.CreateTimeGen()
}

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
	httpmock.RegisterResponder(method, origin+path,
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

func AssertSentMissingCommits(t *testing.T, db kv.DB, fs kv.FileStore, oc <-chan *packutils.Object, sentCommits, commonCommits [][]byte) {
	t.Helper()
	commonTables := map[string]struct{}{}
	commonRows := map[string]struct{}{}
	for _, sum := range commonCommits {
		commit, err := versioning.GetCommit(db, sum)
		require.NoError(t, err)
		commonTables[string(commit.Table)] = struct{}{}
		tbl, err := table.ReadTable(db, fs, commit.Table)
		require.NoError(t, err)
		rhr := tbl.NewRowHashReader(0, 0)
		for {
			_, sum, err := rhr.Read()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			commonRows[string(sum)] = struct{}{}
		}
	}
	commitMap := map[string]struct{}{}
	tableMap := map[string]struct{}{}
	rowMap := map[string]struct{}{}
	for obj := range oc {
		switch obj.Type {
		case encoding.ObjectCommit:
			sum := meow.Checksum(0, obj.Content)
			t.Logf("received commit %x", sum)
			commitMap[string(sum[:])] = struct{}{}
		case encoding.ObjectTable:
			sum := meow.Checksum(0, obj.Content)
			t.Logf("received table %x", sum)
			tableMap[string(sum[:])] = struct{}{}
			_, ok := commonTables[string(sum[:])]
			assert.False(t, ok)
		case encoding.ObjectRow:
			sum := meow.Checksum(0, obj.Content)
			t.Logf("received row %x", sum)
			rowMap[string(sum[:])] = struct{}{}
			_, ok := commonRows[string(sum[:])]
			assert.False(t, ok)
		}
	}
	assert.Equal(t, len(sentCommits), len(commitMap))
	for _, sum := range sentCommits {
		if _, ok := commitMap[string(sum)]; !ok {
			t.Errorf("commit %x not found", sum)
			continue
		}
		commit, err := versioning.GetCommit(db, sum)
		require.NoError(t, err)
		_, ok1 := tableMap[string(commit.Table)]
		_, ok2 := commonTables[string(commit.Table)]
		if !ok1 && !ok2 {
			t.Errorf("table %x not found", commit.Table)
			continue
		}
		tbl, err := table.ReadTable(db, fs, commit.Table)
		require.NoError(t, err)
		rhr := tbl.NewRowHashReader(0, 0)
		for {
			_, sum, err := rhr.Read()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			_, ok1 := rowMap[string(sum)]
			_, ok2 := commonRows[string(sum)]
			if !ok1 && !ok2 {
				t.Errorf("row %x not found", sum)
				break
			}
		}
	}
}

func FetchObjects(t *testing.T, db kv.DB, fs kv.FileStore, advertised [][]byte, havesPerRoundTrip int) <-chan *packutils.Object {
	t.Helper()
	c, err := packclient.NewClient(db, fs, TestOrigin)
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	neg, err := packclient.NewNegotiator(db, fs, &wg, c, advertised, havesPerRoundTrip)
	require.NoError(t, err)
	err = neg.Start()
	require.NoError(t, err)
	return neg.ObjectChan
}

func CopyCommitsToNewStore(t *testing.T, dba, dbb kv.DB, fsa, fsb kv.FileStore, commits [][]byte) {
	t.Helper()
	for _, sum := range commits {
		c, err := versioning.GetCommit(dba, sum)
		require.NoError(t, err)
		_, err = versioning.SaveCommit(dbb, 0, c)
		require.NoError(t, err)
		tbl, err := table.ReadTable(dba, fsa, c.Table)
		require.NoError(t, err)
		builder := table.NewBuilder(dbb, fsb, tbl.Columns(), tbl.PrimaryKeyIndices(), 0, 0)
		r1 := tbl.NewRowReader()
		r2 := tbl.NewRowHashReader(0, 0)
		i := 0
		for {
			pk, sum, err := r2.Read()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			_, rc, err := r1.Read()
			require.NoError(t, err)
			require.NoError(t, builder.InsertRow(i, pk, sum, rc))
			i++
		}
		_, err = builder.SaveTable()
		require.NoError(t, err)
	}
}

func AssertCommitsPersisted(t *testing.T, db kv.DB, fs kv.FileStore, commits [][]byte) {
	t.Helper()
	for _, sum := range commits {
		c, err := versioning.GetCommit(db, sum)
		require.NoError(t, err)
		tbl, err := table.ReadTable(db, fs, c.Table)
		require.NoError(t, err)
		reader := tbl.NewRowReader()
		for {
			_, _, err := reader.Read()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
	}
}

func ReceivePackConfig(denyNonFastForwards, denyDeletes bool) *versioning.Config {
	return &versioning.Config{
		User: &versioning.ConfigUser{
			Name:  "test",
			Email: "test@domain.com",
		},
		Receive: &versioning.ConfigReceive{
			DenyNonFastForwards: &denyNonFastForwards,
			DenyDeletes:         &denyDeletes,
		},
	}
}
