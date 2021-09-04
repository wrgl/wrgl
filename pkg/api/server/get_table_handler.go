// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"encoding/hex"
	"net/http"
	"regexp"

	"github.com/wrgl/core/pkg/api/payload"
	"github.com/wrgl/core/pkg/objects"
)

var tableURIPat = regexp.MustCompile(`/tables/([0-9a-f]{32})/`)

func (s *Server) handleGetTable(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, "forbidden", http.StatusForbidden)
		return
	}
	m := tableURIPat.FindStringSubmatch(r.URL.Path)
	if m == nil {
		http.NotFound(rw, r)
		return
	}
	sum, err := hex.DecodeString(m[1])
	if err != nil {
		panic(err)
	}
	repo := getRepo(r)
	db := s.getDB(repo)
	tbl, err := objects.GetTable(db, sum)
	if err != nil {
		http.NotFound(rw, r)
		return
	}
	resp := &payload.GetTableResponse{
		Columns:   tbl.Columns,
		PK:        tbl.PK,
		RowsCount: tbl.RowsCount,
	}
	writeJSON(rw, resp)
}
