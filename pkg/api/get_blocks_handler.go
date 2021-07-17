// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/hex"
	"net/http"
	"regexp"
	"strconv"

	"github.com/wrgl/core/pkg/objects"
)

const (
	CTCSV          = "text/csv"
	CTBlocksBinary = "application/x-wrgl-blocks-binary"
)

var blocksURIPat = regexp.MustCompile(`/tables/([0-9a-f]{32})/blocks/`)

type GetBlocksHandler struct {
	db objects.Store
}

func NewGetBlocksHandler(db objects.Store) *GetBlocksHandler {
	return &GetBlocksHandler{
		db: db,
	}
}

func (h *GetBlocksHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, "forbidden", http.StatusForbidden)
		return
	}
	m := blocksURIPat.FindStringSubmatch(r.URL.Path)
	if m == nil {
		http.NotFound(rw, r)
		return
	}
	sum, err := hex.DecodeString(m[1])
	if err != nil {
		panic(err)
	}
	tbl, err := objects.GetTable(h.db, sum)
	if err != nil {
		http.NotFound(rw, r)
		return
	}
	blkCount := len(tbl.Blocks)
	values := r.URL.Query()
	start := 0
	if v, ok := values["start"]; ok {
		start, err = strconv.Atoi(v[0])
		if err != nil {
			http.Error(rw, "invalid start", http.StatusBadRequest)
			return
		}
	}
	if start < 0 || start >= int(blkCount) {
		http.Error(rw, "start out of range", http.StatusBadRequest)
		return
	}
	end := blkCount
	if v, ok := values["end"]; ok {
		end, err = strconv.Atoi(v[0])
		if err != nil {
			http.Error(rw, "invalid end", http.StatusBadRequest)
			return
		}
	}
	if end < start || end > int(blkCount) {
		http.Error(rw, "end out of range", http.StatusBadRequest)
		return
	}
	format := "csv"
	if v, ok := values["format"]; ok {
		format = v[0]
	}
	switch format {
	case "binary":
		rw.Header().Set("Content-Encoding", "gzip")
		gzw := gzip.NewWriter(rw)
		defer gzw.Close()
		rw.Header().Set("Content-Type", CTBlocksBinary)
		for i := start; i < end; i++ {
			b, err := objects.GetBlockBytes(h.db, tbl.Blocks[i])
			if err != nil {
				panic(err)
			}
			_, err = gzw.Write(b)
			if err != nil {
				panic(err)
			}
		}
	case "csv":
		rw.Header().Set("Content-Encoding", "gzip")
		gzw := gzip.NewWriter(rw)
		defer gzw.Close()
		rw.Header().Set("Content-Type", CTCSV)
		w := csv.NewWriter(gzw)
		defer w.Flush()
		for i := start; i < end; i++ {
			blk, err := objects.GetBlock(h.db, tbl.Blocks[i])
			if err != nil {
				panic(err)
			}
			err = w.WriteAll(blk)
			if err != nil {
				panic(err)
			}
		}
	default:
		http.Error(rw, "invalid format", http.StatusBadRequest)
		return
	}
}
