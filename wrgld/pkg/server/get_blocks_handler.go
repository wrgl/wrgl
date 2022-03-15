package server

import (
	"encoding/csv"
	"encoding/hex"
	"net/http"
	"regexp"
	"strconv"

	"github.com/klauspost/compress/gzip"
	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/encoding/packfile"
	"github.com/wrgl/wrgl/pkg/objects"
)

var blocksURIPat = regexp.MustCompile(`/tables/([0-9a-f]{32})/blocks/`)

func (s *Server) transferBlocks(rw http.ResponseWriter, r *http.Request, db objects.Store, tblProf []byte) {
	tbl, err := objects.GetTable(db, tblProf)
	if err != nil {
		SendHTTPError(rw, http.StatusNotFound)
		return
	}
	blkCount := len(tbl.Blocks)
	values := r.URL.Query()
	start := 0
	if v, ok := values["start"]; ok {
		start, err = strconv.Atoi(v[0])
		if err != nil {
			SendError(rw, http.StatusBadRequest, "invalid start")
			return
		}
	}
	if start < 0 || start >= int(blkCount) {
		SendError(rw, http.StatusBadRequest, "start out of range")
		return
	}
	end := blkCount
	if v, ok := values["end"]; ok {
		end, err = strconv.Atoi(v[0])
		if err != nil {
			SendError(rw, http.StatusBadRequest, "invalid end")
			return
		}
	}
	if end < start || end > int(blkCount) {
		SendError(rw, http.StatusBadRequest, "end out of range")
		return
	}
	format := payload.BlockFormatCSV
	if v, ok := values["format"]; ok {
		format = payload.BlockFormat(v[0])
	}
	s.cacheControlImmutable(rw)
	switch format {
	case payload.BlockFormatBinary:
		rw.Header().Set("Content-Type", api.CTPackfile)
		pw, err := packfile.NewPackfileWriter(rw)
		if err != nil {
			return
		}
		for i := start; i < end; i++ {
			b, err := objects.GetBlockBytes(db, tbl.Blocks[i])
			if err != nil {
				panic(err)
			}
			_, err = pw.WriteObject(packfile.ObjectBlock, b)
			if err != nil {
				panic(err)
			}
		}
	case payload.BlockFormatCSV:
		rw.Header().Set("Content-Encoding", "gzip")
		gzw, err := gzip.NewWriterLevel(rw, 4)
		if err != nil {
			panic(err)
		}
		defer gzw.Close()
		rw.Header().Set("Content-Type", api.CTCSV)
		w := csv.NewWriter(gzw)
		defer w.Flush()
		if v, ok := values["columns"]; ok && v[0] == "true" {
			if err := w.Write(tbl.Columns); err != nil {
				panic(err)
			}
		}
		var buf []byte
		var blk [][]string
		for i := start; i < end; i++ {
			blk, buf, err = objects.GetBlock(db, buf, tbl.Blocks[i])
			if err != nil {
				panic(err)
			}
			err = w.WriteAll(blk)
			if err != nil {
				panic(err)
			}
		}
	default:
		SendError(rw, http.StatusBadRequest, "invalid format")
		return
	}
}

func (s *Server) handleGetBlocks(rw http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	sum := s.getCommitSum(rw, r, values, "head")
	if sum == nil {
		return
	}
	db := s.getDB(r)
	com, err := objects.GetCommit(db, sum)
	if err != nil {
		SendHTTPError(rw, http.StatusNotFound)
		return
	}
	s.transferBlocks(rw, r, db, com.Table)
}

func (s *Server) handleGetTableBlocks(rw http.ResponseWriter, r *http.Request) {
	m := blocksURIPat.FindStringSubmatch(r.URL.Path)
	if m == nil {
		SendHTTPError(rw, http.StatusNotFound)
		return
	}
	sum, err := hex.DecodeString(m[1])
	if err != nil {
		panic(err)
	}
	db := s.getDB(r)
	s.transferBlocks(rw, r, db, sum)
}
