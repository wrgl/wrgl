// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/wrgl/core/pkg/api"
	"github.com/wrgl/core/pkg/diff"
	"github.com/wrgl/core/pkg/objects"
)

var rowsURIPat = regexp.MustCompile(`/tables/([0-9a-f]{32})/rows/`)

func (s *Server) handleGetRows(rw http.ResponseWriter, r *http.Request) {
	m := rowsURIPat.FindStringSubmatch(r.URL.Path)
	if m == nil {
		http.NotFound(rw, r)
		return
	}
	sum, err := hex.DecodeString(m[1])
	if err != nil {
		panic(err)
	}
	db := s.getDB(r)
	tbl, err := objects.GetTable(db, sum)
	if err != nil {
		http.NotFound(rw, r)
		return
	}
	values := r.URL.Query()
	var offsets []uint32
	if v, ok := values["offsets"]; ok {
		sl := strings.Split(v[0], ",")
		offsets = make([]uint32, len(sl))
		for i, s := range sl {
			u, err := strconv.Atoi(s)
			if err != nil {
				http.Error(rw, fmt.Sprintf("invalid offset %q", s), http.StatusBadRequest)
				return
			}
			if u < 0 || u > int(tbl.RowsCount) {
				http.Error(rw, fmt.Sprintf("offset out of range %q", s), http.StatusBadRequest)
				return
			}
			offsets[i] = uint32(u)
		}
	}
	if len(offsets) == 0 {
		// redirect to blocks endpoint to download everything
		url := &url.URL{}
		*url = *r.URL
		url.Path = fmt.Sprintf("/tables/%x/blocks/", sum)
		http.Redirect(rw, r, url.String(), http.StatusTemporaryRedirect)
		return
	}
	buf, err := diff.NewBlockBuffer([]objects.Store{db}, []*objects.Table{tbl})
	if err != nil {
		panic(err)
	}
	s.cacheControlImmutable(rw)
	rw.Header().Set("Content-Encoding", "gzip")
	gzw := gzip.NewWriter(rw)
	defer gzw.Close()
	rw.Header().Set("Content-Type", api.CTCSV)
	w := csv.NewWriter(gzw)
	defer w.Flush()
	for _, o := range offsets {
		blk, row := diff.RowToBlockAndOffset(o)
		strs, err := buf.GetRow(0, blk, row)
		if err != nil {
			panic(err)
		}
		err = w.Write(strs)
		if err != nil {
			panic(err)
		}
	}
}
