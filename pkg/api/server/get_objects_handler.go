// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiserver

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"

	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/encoding/packfile"
	"github.com/wrgl/wrgl/pkg/objects"
)

func (s *Server) handleGetObjects(rw http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	db := s.getDB(r)
	v, ok := values["tables"]
	if !ok {
		return
	}
	hexes := strings.Split(v[0], ",")
	tables := make([]*objects.Table, len(hexes))
	for i, h := range hexes {
		sum, err := hex.DecodeString(h)
		if err != nil {
			SendError(rw, http.StatusBadRequest, fmt.Sprintf("error decoding table sum: %v", err))
			return
		}
		tbl, err := objects.GetTable(db, sum)
		if err == objects.ErrKeyNotFound {
			SendError(rw, http.StatusNotFound, fmt.Sprintf("table %s not found", h))
			return
		}
		if err != nil {
			panic(err)
		}
		tables[i] = tbl
	}

	rw.Header().Set("Content-Type", api.CTPackfile)
	rw.Header().Set("Content-Encoding", "gzip")

	gzw := gzip.NewWriter(rw)
	defer gzw.Close()

	pw, err := packfile.NewPackfileWriter(gzw)
	if err != nil {
		panic(err)
	}
	buf := bytes.NewBuffer(nil)
	for _, tbl := range tables {
		for _, blk := range tbl.Blocks {
			b, err := objects.GetBlockBytes(db, blk)
			if err != nil {
				panic(err)
			}
			_, err = pw.WriteObject(packfile.ObjectBlock, b)
			if err != nil {
				panic(err)
			}
		}
		buf.Reset()
		_, err = tbl.WriteTo(buf)
		if err != nil {
			panic(err)
		}
		_, err = pw.WriteObject(packfile.ObjectTable, buf.Bytes())
		if err != nil {
			panic(err)
		}
	}
}
