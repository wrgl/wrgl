// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/hex"
	"net/http"
	"regexp"
	"strconv"

	"github.com/wrgl/core/pkg/api"
	"github.com/wrgl/core/pkg/api/payload"
	"github.com/wrgl/core/pkg/objects"
)

var blocksURIPat = regexp.MustCompile(`/tables/([0-9a-f]{32})/blocks/`)

func (s *Server) handleGetBlocks(rw http.ResponseWriter, r *http.Request) {
	m := blocksURIPat.FindStringSubmatch(r.URL.Path)
	if m == nil {
		sendHTTPError(rw, http.StatusNotFound)
		return
	}
	sum, err := hex.DecodeString(m[1])
	if err != nil {
		panic(err)
	}
	db := s.getDB(r)
	tbl, err := objects.GetTable(db, sum)
	if err != nil {
		sendHTTPError(rw, http.StatusNotFound)
		return
	}
	blkCount := len(tbl.Blocks)
	values := r.URL.Query()
	start := 0
	if v, ok := values["start"]; ok {
		start, err = strconv.Atoi(v[0])
		if err != nil {
			sendError(rw, http.StatusBadRequest, "invalid start")
			return
		}
	}
	if start < 0 || start >= int(blkCount) {
		sendError(rw, http.StatusBadRequest, "start out of range")
		return
	}
	end := blkCount
	if v, ok := values["end"]; ok {
		end, err = strconv.Atoi(v[0])
		if err != nil {
			sendError(rw, http.StatusBadRequest, "invalid end")
			return
		}
	}
	if end < start || end > int(blkCount) {
		sendError(rw, http.StatusBadRequest, "end out of range")
		return
	}
	format := payload.BlockFormatCSV
	if v, ok := values["format"]; ok {
		format = payload.BlockFormat(v[0])
	}
	s.cacheControlImmutable(rw)
	switch format {
	case payload.BlockFormatBinary:
		rw.Header().Set("Content-Encoding", "gzip")
		gzw := gzip.NewWriter(rw)
		defer gzw.Close()
		rw.Header().Set("Content-Type", api.CTBlocksBinary)
		for i := start; i < end; i++ {
			b, err := objects.GetBlockBytes(db, tbl.Blocks[i])
			if err != nil {
				panic(err)
			}
			_, err = gzw.Write(b)
			if err != nil {
				panic(err)
			}
		}
	case payload.BlockFormatCSV:
		rw.Header().Set("Content-Encoding", "gzip")
		gzw := gzip.NewWriter(rw)
		defer gzw.Close()
		rw.Header().Set("Content-Type", api.CTCSV)
		w := csv.NewWriter(gzw)
		defer w.Flush()
		for i := start; i < end; i++ {
			blk, err := objects.GetBlock(db, tbl.Blocks[i])
			if err != nil {
				panic(err)
			}
			err = w.WriteAll(blk)
			if err != nil {
				panic(err)
			}
		}
	default:
		sendError(rw, http.StatusBadRequest, "invalid format")
		return
	}
}
