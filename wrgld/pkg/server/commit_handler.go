package server

import (
	"bytes"
	"encoding/csv"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/gzip"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/sorter"
	"github.com/wrgl/wrgl/pkg/transaction"
)

func (s *Server) handleCommit(rw http.ResponseWriter, r *http.Request) {
	email := GetEmail(r)
	if email == "" {
		SendHTTPError(rw, http.StatusUnauthorized)
		return
	}
	err := r.ParseMultipartForm(0)
	if err != nil {
		if err == http.ErrNotMultipart || err == http.ErrMissingBoundary {
			SendError(rw, http.StatusUnsupportedMediaType, err.Error())
			return
		}
		panic(err)
	}
	branch := r.PostFormValue("branch")
	if branch == "" {
		SendError(rw, http.StatusBadRequest, "missing branch name")
		return
	}
	if !ref.HeadPattern.MatchString(branch) {
		SendError(rw, http.StatusBadRequest, "invalid branch name")
		return
	}
	message := r.PostFormValue("message")
	if message == "" {
		SendError(rw, http.StatusBadRequest, "missing message")
		return
	}
	if len(r.MultipartForm.File["file"]) == 0 {
		SendError(rw, http.StatusBadRequest, "missing file")
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
	var primaryKey []string
	if sl := r.MultipartForm.Value["primaryKey"]; len(sl) > 0 && len(sl[0]) > 0 {
		primaryKey = strings.Split(sl[0], ",")
	}
	db := s.getDB(r)

	var tid *uuid.UUID
	if s := r.PostFormValue("txid"); s != "" {
		tid = &uuid.UUID{}
		*tid, err = uuid.Parse(s)
		if err != nil {
			SendError(rw, http.StatusBadRequest, "invalid txid")
			return
		}
		if !objects.TransactionExist(db, *tid) {
			SendError(rw, http.StatusNotFound, "transaction not found")
			return
		}
	}

	var opts = []ingest.InserterOption{}
	if s.debugOut != nil {
		opts = append(opts, ingest.WithDebugOutput(s.debugOut))
	}
	sorter := s.sPool.Get().(*sorter.Sorter)
	sorter.Reset()
	defer s.sPool.Put(sorter)
	sum, err := ingest.IngestTable(db, sorter, f, primaryKey, opts...)
	if err != nil {
		if v, ok := err.(*csv.ParseError); ok {
			sendCSVError(rw, v)
			return
		} else {
			panic(err)
		}
	}

	name := GetName(r)
	commit := &objects.Commit{
		Table:       sum,
		Message:     message,
		Time:        time.Now(),
		AuthorEmail: email,
		AuthorName:  name,
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
	if tid != nil {
		if err = transaction.Add(rs, *tid, branch, commitSum); err != nil {
			panic(err)
		}
	} else if err = ref.CommitHead(rs, branch, commitSum, commit); err != nil {
		panic(err)
	}
	if s.postCommit != nil {
		s.postCommit(r, commit, commitSum, branch, tid)
	}
	resp := &payload.CommitResponse{
		Sum:   &payload.Hex{},
		Table: &payload.Hex{},
	}
	copy((*resp.Sum)[:], commitSum)
	copy((*resp.Table)[:], sum)
	WriteJSON(rw, resp)
}
