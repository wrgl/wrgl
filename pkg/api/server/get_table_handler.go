// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"encoding/hex"
	"net/http"
	"regexp"

	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/objects"
)

var tableURIPat = regexp.MustCompile(`/tables/([0-9a-f]{32})/`)

func (s *Server) handleGetTable(rw http.ResponseWriter, r *http.Request) {
	m := tableURIPat.FindStringSubmatch(r.URL.Path)
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
	resp := &payload.GetTableResponse{
		Columns:   tbl.Columns,
		PK:        tbl.PK,
		RowsCount: tbl.RowsCount,
	}
	s.cacheControlImmutable(rw)
	writeJSON(rw, resp)
}
