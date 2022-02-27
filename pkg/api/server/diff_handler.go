// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"bytes"
	"encoding/hex"
	"net/http"
	"regexp"

	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/diff"
	diffprof "github.com/wrgl/wrgl/pkg/diff/prof"
	"github.com/wrgl/wrgl/pkg/objects"
)

var diffURIPat = regexp.MustCompile(`/diff/([0-9a-f]{32})/([0-9a-f]{32})/`)

func (s *Server) getTable(db objects.Store, x string) ([]byte, *objects.Table, [][]string) {
	sum, err := hex.DecodeString(x)
	if err != nil {
		panic(err)
	}
	com, err := objects.GetCommit(db, sum)
	if err != nil {
		return nil, nil, nil
	}
	tbl, err := objects.GetTable(db, com.Table)
	if err != nil {
		return nil, nil, nil
	}
	idx, err := objects.GetTableIndex(db, com.Table)
	if err != nil {
		panic(err)
	}
	return com.Table, tbl, idx
}

func diffDataProfile(db objects.Store, resp *payload.DiffResponse, sum1, sum2 []byte) {
	newProf, err := objects.GetTableProfile(db, sum1)
	if err != nil {
		return
	}
	oldProf, err := objects.GetTableProfile(db, sum2)
	if err != nil {
		return
	}
	resp.DataProfile = diffprof.DiffTableProfiles(newProf, oldProf)
}

func (s *Server) handleDiff(rw http.ResponseWriter, r *http.Request) {
	m := diffURIPat.FindStringSubmatch(r.URL.Path)
	if m == nil {
		SendHTTPError(rw, http.StatusNotFound)
		return
	}
	db := s.getDB(r)
	sum1, tbl1, idx1 := s.getTable(db, m[1])
	if tbl1 == nil {
		SendHTTPError(rw, http.StatusNotFound)
		return
	}
	sum2, tbl2, idx2 := s.getTable(db, m[2])
	if tbl2 == nil {
		SendHTTPError(rw, http.StatusNotFound)
		return
	}
	resp := &payload.DiffResponse{
		TableSum:    payload.BytesToHex(sum1),
		OldTableSum: payload.BytesToHex(sum2),
		Columns:     tbl1.Columns,
		OldColumns:  tbl2.Columns,
		PK:          tbl1.PK,
		OldPK:       tbl2.PK,
	}
	if !bytes.Equal(sum1, sum2) {
		errCh := make(chan error, 10)
		opts := []diff.DiffOption{}
		if s.debugOut != nil {
			opts = append(opts, diff.WithDebugOutput(s.debugOut))
		}
		diffChan, _ := diff.DiffTables(db, db, tbl1, tbl2, idx1, idx2, errCh, opts...)
		for obj := range diffChan {
			rd := &payload.RowDiff{}
			if obj.Sum != nil {
				u := obj.Offset
				rd.Offset1 = &u
			}
			if obj.OldSum != nil {
				u := obj.OldOffset
				rd.Offset2 = &u
			}
			resp.RowDiff = append(resp.RowDiff, rd)
		}
		close(errCh)
		err, ok := <-errCh
		if ok {
			panic(err)
		}
		diffDataProfile(db, resp, sum1, sum2)
	}
	s.cacheControlImmutable(rw)
	WriteJSON(rw, resp)
}
