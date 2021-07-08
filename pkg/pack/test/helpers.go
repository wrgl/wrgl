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
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/objects"
	packclient "github.com/wrgl/core/pkg/pack/client"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/testutils"
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

func AssertSentMissingCommits(t *testing.T, db objects.Store, oc <-chan *packutils.Object, sentCommits, commonCommits [][]byte) {
	t.Helper()
	commonTables := map[string]struct{}{}
	commonBlocks := map[string]struct{}{}
	for _, sum := range commonCommits {
		commit, err := objects.GetCommit(db, sum)
		require.NoError(t, err)
		commonTables[string(commit.Table)] = struct{}{}
		tbl, err := objects.GetTable(db, commit.Table)
		require.NoError(t, err)
		for _, blk := range tbl.Blocks {
			commonBlocks[string(blk)] = struct{}{}
		}
	}
	commitMap := map[string]struct{}{}
	tableMap := map[string]struct{}{}
	blockMap := map[string]struct{}{}
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
		case encoding.ObjectBlock:
			sum := meow.Checksum(0, obj.Content)
			t.Logf("received block %x", sum)
			blockMap[string(sum[:])] = struct{}{}
			_, ok := commonBlocks[string(sum[:])]
			assert.False(t, ok)
		}
	}
	assert.Equal(t, len(sentCommits), len(commitMap))
	for _, sum := range sentCommits {
		if _, ok := commitMap[string(sum)]; !ok {
			t.Errorf("commit %x not found", sum)
			continue
		}
		commit, err := objects.GetCommit(db, sum)
		require.NoError(t, err)
		_, ok1 := tableMap[string(commit.Table)]
		_, ok2 := commonTables[string(commit.Table)]
		if !ok1 && !ok2 {
			t.Errorf("table %x not found", commit.Table)
			continue
		}
		tbl, err := objects.GetTable(db, commit.Table)
		require.NoError(t, err)
		for _, blk := range tbl.Blocks {
			_, ok1 := blockMap[string(blk)]
			_, ok2 := commonBlocks[string(blk)]
			if !ok1 && !ok2 {
				t.Errorf("block %x not found", sum)
				break
			}
		}
	}
}

func FetchObjects(t *testing.T, db objects.Store, rs ref.Store, advertised [][]byte, havesPerRoundTrip int) <-chan *packutils.Object {
	t.Helper()
	c, err := packclient.NewClient(db, TestOrigin)
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	neg, err := packclient.NewNegotiator(db, rs, &wg, c, advertised, havesPerRoundTrip)
	require.NoError(t, err)
	err = neg.Start()
	require.NoError(t, err)
	return neg.ObjectChan
}

func CopyCommitsToNewStore(t *testing.T, dba, dbb objects.Store, commits [][]byte) {
	t.Helper()
	enc := objects.NewStrListEncoder(true)
	for _, sum := range commits {
		c, err := objects.GetCommit(dba, sum)
		require.NoError(t, err)
		tbl, err := objects.GetTable(dba, c.Table)
		require.NoError(t, err)
		buf := bytes.NewBuffer(nil)
		for _, sum := range tbl.Blocks {
			blk, err := objects.GetBlock(dba, sum)
			require.NoError(t, err)
			buf.Reset()
			_, err = objects.WriteBlockTo(enc, buf, blk)
			require.NoError(t, err)
			_, err = objects.SaveBlock(dbb, buf.Bytes())
			require.NoError(t, err)
		}
		buf.Reset()
		_, err = tbl.WriteTo(buf)
		require.NoError(t, err)
		_, err = objects.SaveTable(dbb, buf.Bytes())
		require.NoError(t, err)
		buf.Reset()
		_, err = c.WriteTo(buf)
		require.NoError(t, err)
		_, err = objects.SaveCommit(dbb, buf.Bytes())
		require.NoError(t, err)
		require.NoError(t, ingest.IndexTable(dbb, c.Table, tbl))
	}
}

func AssertCommitsPersisted(t *testing.T, db objects.Store, rs ref.Store, commits [][]byte) {
	t.Helper()
	for _, sum := range commits {
		c, err := objects.GetCommit(db, sum)
		require.NoError(t, err)
		tbl, err := objects.GetTable(db, c.Table)
		require.NoError(t, err)
		for _, blk := range tbl.Blocks {
			assert.True(t, objects.BlockExist(db, blk))
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
