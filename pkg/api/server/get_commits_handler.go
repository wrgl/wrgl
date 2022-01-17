package apiserver

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"

	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

var sumRegexp = regexp.MustCompile(`^[0-9a-f]{32}$`)

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

func CommitPayload(db objects.Store, com *objects.Commit) *payload.Commit {
	obj := &payload.Commit{
		AuthorName:  com.AuthorName,
		AuthorEmail: com.AuthorEmail,
		Message:     com.Message,
		Table: &payload.Table{
			Sum:   payload.BytesToHex(com.Table),
			Exist: objects.TableExist(db, com.Table),
		},
		Time: com.Time,
	}
	for _, sum := range com.Parents {
		obj.Parents = append(obj.Parents, payload.BytesToHex(sum))
	}
	return obj
}

func getCommitTree(db objects.Store, sum []byte, maxDepth int) (*payload.Commit, error) {
	result := map[string]*payload.Commit{}
	commits := []*payload.Commit{}
	com, err := objects.GetCommit(db, sum)
	if err != nil {
		return nil, err
	}
	obj := CommitPayload(db, com)
	commits = append(commits, obj)
	result[hex.EncodeToString(sum)] = obj

	for i := 1; i <= maxDepth; i++ {
		next := []*payload.Commit{}
		for _, com := range commits {
			for _, sum := range com.Parents {
				parent, err := objects.GetCommit(db, (*sum)[:])
				if err != nil {
					return nil, err
				}
				obj := CommitPayload(db, parent)
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
	return obj, nil
}

func (s *Server) getHeadSum(rw http.ResponseWriter, r *http.Request, query url.Values) (sum []byte) {
	rs := s.getRS(r)
	if s := query.Get("head"); s == "" {
		sendError(rw, http.StatusBadRequest, "missing head query param")
		return nil
	} else if sumRegexp.MatchString(s) {
		sum, _ = hex.DecodeString(s)
	} else {
		var err error
		sum, err = ref.GetRef(rs, s)
		if err != nil {
			sendHTTPError(rw, http.StatusNotFound)
			return nil
		}
	}
	return sum
}

func (s *Server) handleGetCommits(rw http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	sum := s.getHeadSum(rw, r, query)
	if sum == nil {
		return
	}
	maxDepth, err := getQueryInt(query, "maxDepth", 20)
	if err != nil {
		sendError(rw, http.StatusBadRequest, err.Error())
		return
	}

	db := s.getDB(r)
	root, err := getCommitTree(db, sum, maxDepth)
	if err != nil {
		panic(err)
	}
	resp := &payload.GetCommitsResponse{
		Sum:  payload.BytesToHex(sum),
		Root: *root,
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
