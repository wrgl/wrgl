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

var commitURIPat = regexp.MustCompile(`/commits/([0-9a-f]{32})/`)

func writeCommitJSON(rw http.ResponseWriter, r *http.Request, db objects.Store, sum []byte) {
	com, err := objects.GetCommit(db, sum)
	if err != nil {
		sendHTTPError(rw, http.StatusNotFound)
		return
	}
	tbl, err := objects.GetTable(db, com.Table)
	if err != nil {
		panic(err)
	}
	resp := &payload.Commit{
		Sum:         payload.BytesToHex(sum),
		AuthorName:  com.AuthorName,
		AuthorEmail: com.AuthorEmail,
		Message:     com.Message,
		Time:        com.Time,
		Table: &payload.Table{
			Sum:       &payload.Hex{},
			Columns:   tbl.Columns,
			RowsCount: tbl.RowsCount,
			PK:        tbl.PK,
		},
	}
	copy((*resp.Table.Sum)[:], com.Table)
	for _, sum := range com.Parents {
		h := &payload.Hex{}
		copy((*h)[:], sum)
		resp.Parents = append(resp.Parents, h)
	}
	writeJSON(rw, resp)
}

func (s *Server) handleGetCommit(rw http.ResponseWriter, r *http.Request) {
	m := commitURIPat.FindStringSubmatch(r.URL.Path)
	if m == nil {
		sendHTTPError(rw, http.StatusNotFound)
		return
	}
	sum, err := hex.DecodeString(m[1])
	if err != nil {
		panic(err)
	}
	db := s.getDB(r)
	s.cacheControlImmutable(rw)
	writeCommitJSON(rw, r, db, sum)
}
