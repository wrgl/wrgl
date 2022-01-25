// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/klauspost/compress/gzip"
	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/diff"
	"github.com/wrgl/wrgl/pkg/objects"
)

var rowsURIPat = regexp.MustCompile(`/tables/([0-9a-f]{32})/rows/`)

func (s *Server) transferRows(rw http.ResponseWriter, r *http.Request, db objects.Store, sum []byte) {
	tbl, err := objects.GetTable(db, sum)
	if err != nil {
		sendHTTPError(rw, http.StatusNotFound)
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
				sendError(rw, http.StatusBadRequest, fmt.Sprintf("invalid offset %q", s))
				return
			}
			if u < 0 || u > int(tbl.RowsCount) {
				sendError(rw, http.StatusBadRequest, fmt.Sprintf("offset out of range %q", s))
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
	gzw, err := gzip.NewWriterLevel(rw, 4)
	if err != nil {
		panic(err)
	}
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

func (s *Server) handleGetRows(rw http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	sum := s.getCommitSum(rw, r, values, "head")
	if sum == nil {
		return
	}
	db := s.getDB(r)
	com, err := objects.GetCommit(db, sum)
	if err != nil {
		sendHTTPError(rw, http.StatusNotFound)
		return
	}
	s.transferRows(rw, r, db, com.Table)
}

func (s *Server) handleGetTableRows(rw http.ResponseWriter, r *http.Request) {
	m := rowsURIPat.FindStringSubmatch(r.URL.Path)
	if m == nil {
		sendHTTPError(rw, http.StatusNotFound)
		return
	}
	sum, err := hex.DecodeString(m[1])
	if err != nil {
		panic(err)
	}
	db := s.getDB(r)
	s.transferRows(rw, r, db, sum)
}
