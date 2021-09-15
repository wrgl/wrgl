// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"bytes"
	"encoding/csv"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/wrgl/core/pkg/api/payload"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

func (s *Server) handleCommit(rw http.ResponseWriter, r *http.Request) {
	claims := getClaims(r)
	err := r.ParseMultipartForm(0)
	if err != nil {
		if err == http.ErrNotMultipart || err == http.ErrMissingBoundary {
			sendError(rw, http.StatusUnsupportedMediaType, err.Error())
			return
		}
		panic(err)
	}
	branch := r.PostFormValue("branch")
	if branch == "" {
		sendError(rw, http.StatusBadRequest, "missing branch name")
		return
	}
	if !ref.HeadPattern.MatchString(branch) {
		sendError(rw, http.StatusBadRequest, "invalid branch name")
		return
	}
	message := r.PostFormValue("message")
	if message == "" {
		sendError(rw, http.StatusBadRequest, "missing message")
		return
	}
	if len(r.MultipartForm.File["file"]) == 0 {
		sendError(rw, http.StatusBadRequest, "missing file")
		return
	}
	fh := r.MultipartForm.File["file"][0]
	var f io.ReadCloser
	f, err = fh.Open()
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if strings.HasSuffix(fh.Filename, ".gz") {
		f, err = gzip.NewReader(f)
		if err != nil {
			panic(err)
		}
		defer f.Close()
	}
	primaryKey := r.MultipartForm.Value["primaryKey"]

	db := s.getDB(r)
	sum, err := ingest.IngestTable(db, f, primaryKey, 0, 1, nil, nil)
	if err != nil {
		if v, ok := err.(*csv.ParseError); ok {
			sendCSVError(rw, v)
			return
		} else {
			panic(err)
		}
	}

	commit := &objects.Commit{
		Table:       sum,
		Message:     message,
		Time:        time.Now(),
		AuthorEmail: claims.Email,
		AuthorName:  claims.Name,
	}
	rs := s.getRS(r)
	parent, _ := ref.GetHead(rs, branch)
	if parent != nil {
		commit.Parents = [][]byte{parent}
	}
	buf := bytes.NewBuffer(nil)
	_, err = commit.WriteTo(buf)
	if err != nil {
		panic(err)
	}
	commitSum, err := objects.SaveCommit(db, buf.Bytes())
	if err != nil {
		panic(err)
	}
	err = ref.CommitHead(rs, branch, commitSum, commit)
	if err != nil {
		panic(err)
	}
	if s.postCommit != nil {
		s.postCommit(r, commit, commitSum, branch)
	}
	resp := &payload.CommitResponse{
		Sum:   &payload.Hex{},
		Table: &payload.Hex{},
	}
	copy((*resp.Sum)[:], commitSum)
	copy((*resp.Table)[:], sum)
	writeJSON(rw, resp)
}
