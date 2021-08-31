// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"bytes"
	"net/http"
	"time"

	"github.com/wrgl/core/pkg/api/payload"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

type CommitHandler struct {
	db         objects.Store
	rs         ref.Store
	postCommit func(commit *objects.Commit, sum []byte, branch string)
}

type CommitHandlerOption func(h *CommitHandler)

func WithPostCommitCallback(postCommit func(commit *objects.Commit, sum []byte, branch string)) CommitHandlerOption {
	return func(h *CommitHandler) {
		h.postCommit = postCommit
	}
}

func NewCommitHandler(db objects.Store, rs ref.Store, opts ...CommitHandlerOption) *CommitHandler {
	h := &CommitHandler{
		db: db,
		rs: rs,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (h *CommitHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "forbidden", http.StatusForbidden)
		return
	}
	err := r.ParseMultipartForm(0)
	if err != nil {
		if err == http.ErrNotMultipart || err == http.ErrMissingBoundary {
			http.Error(rw, err.Error(), http.StatusUnsupportedMediaType)
			return
		}
		panic(err)
	}
	branch := r.PostFormValue("branch")
	if branch == "" {
		http.Error(rw, "missing branch name", http.StatusBadRequest)
		return
	}
	if !ref.HeadPattern.MatchString(branch) {
		http.Error(rw, "invalid branch name", http.StatusBadRequest)
		return
	}
	message := r.PostFormValue("message")
	if message == "" {
		http.Error(rw, "missing message", http.StatusBadRequest)
		return
	}
	email := r.PostFormValue("authorEmail")
	if email == "" {
		http.Error(rw, "missing author email", http.StatusBadRequest)
		return
	}
	name := r.PostFormValue("authorName")
	if name == "" {
		http.Error(rw, "missing author name", http.StatusBadRequest)
		return
	}
	if len(r.MultipartForm.File["file"]) == 0 {
		http.Error(rw, "missing file", http.StatusBadRequest)
		return
	}
	fh := r.MultipartForm.File["file"][0]
	f, err := fh.Open()
	if err != nil {
		panic(err)
	}
	primaryKey := r.MultipartForm.Value["primaryKey"]

	sum, err := ingest.IngestTable(h.db, f, primaryKey, 0, 1, nil, nil)
	if err != nil {
		panic(err)
	}

	commit := &objects.Commit{
		Table:       sum,
		Message:     message,
		Time:        time.Now(),
		AuthorEmail: email,
		AuthorName:  name,
	}
	parent, _ := ref.GetHead(h.rs, branch)
	if parent != nil {
		commit.Parents = [][]byte{parent}
	}
	buf := bytes.NewBuffer(nil)
	_, err = commit.WriteTo(buf)
	if err != nil {
		panic(err)
	}
	commitSum, err := objects.SaveCommit(h.db, buf.Bytes())
	if err != nil {
		panic(err)
	}
	err = ref.CommitHead(h.rs, branch, commitSum, commit)
	if err != nil {
		panic(err)
	}
	if h.postCommit != nil {
		h.postCommit(commit, commitSum, branch)
	}
	resp := &payload.CommitResponse{
		Sum:   &payload.Hex{},
		Table: &payload.Hex{},
	}
	copy((*resp.Sum)[:], commitSum)
	copy((*resp.Table)[:], sum)
	writeJSON(rw, resp)
}
