package apiserver

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/wrgl/core/pkg/api"
	"github.com/wrgl/core/pkg/api/payload"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

func getQueryInt(query url.Values, key string, initial int) (res int, err error) {
	res = initial
	if v := query.Get(key); v != "" {
		res, err = strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("invalid " + key)
		}
	}
	return
}

func CommitPayload(com *objects.Commit) *payload.Commit {
	obj := &payload.Commit{
		AuthorName:  com.AuthorName,
		AuthorEmail: com.AuthorEmail,
		Message:     com.Message,
		Table:       payload.BytesToHex(com.Table),
		Time:        com.Time,
	}
	for _, sum := range com.Parents {
		obj.Parents = append(obj.Parents, payload.BytesToHex(sum))
	}
	return obj
}

func getCommitTree(db objects.Store, sum []byte, minDepth, maxDepth int) (map[string]*payload.Commit, error) {
	if maxDepth < minDepth {
		return nil, nil
	}
	current := [][]byte{sum}
	for i := 0; i < minDepth; i++ {
		next := [][]byte{}
		for _, sum := range current {
			com, err := objects.GetCommit(db, sum)
			if err != nil {
				return nil, err
			}
			next = append(next, com.Parents...)
		}
		if len(next) == 0 {
			return nil, nil
		}
		current = next
	}

	result := map[string]*payload.Commit{}
	commits := []*payload.Commit{}
	for _, sum := range current {
		com, err := objects.GetCommit(db, sum)
		if err != nil {
			return nil, err
		}
		obj := CommitPayload(com)
		commits = append(commits, obj)
		result[hex.EncodeToString(sum)] = obj
	}

	for i := minDepth + 1; i <= maxDepth; i++ {
		next := []*payload.Commit{}
		for _, com := range commits {
			for _, sum := range com.Parents {
				parent, err := objects.GetCommit(db, (*sum)[:])
				if err != nil {
					return nil, err
				}
				obj := CommitPayload(parent)
				if com.ParentCommits == nil {
					com.ParentCommits = map[string]*payload.Commit{}
				}
				com.ParentCommits[hex.EncodeToString((*sum)[:])] = obj
				next = append(next, obj)
			}
		}
		if len(next) == 0 {
			break
		}
		commits = next
	}
	return result, nil
}

func (s *Server) handleGetCommits(rw http.ResponseWriter, r *http.Request) {
	var sum []byte
	var err error

	rs := s.getRS(r)
	query := r.URL.Query()
	if s := query.Get("ref"); s == "" {
		http.Error(rw, "missing ref query param", http.StatusBadRequest)
		return
	} else {
		sum, err = ref.GetRef(rs, s)
		if err != nil {
			http.NotFound(rw, r)
			return
		}
	}
	minDepth, err := getQueryInt(query, "minDepth", 0)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	maxDepth, err := getQueryInt(query, "maxDepth", 0)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	db := s.getDB(r)
	commits, err := getCommitTree(db, sum, minDepth, maxDepth)
	if err != nil {
		panic(err)
	}
	resp := &payload.GetCommitsResponse{
		Commits: commits,
	}
	b, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	rw.Header().Set("Content-Type", api.CTJSON)
	_, err = rw.Write(b)
	if err != nil {
		panic(err)
	}
}
