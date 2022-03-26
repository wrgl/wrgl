package server

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/transaction"
)

func parseJSONRequest(r *http.Request, rw http.ResponseWriter, obj interface{}) bool {
	if v := r.Header.Get("Content-Type"); !strings.Contains(v, api.CTJSON) {
		SendError(rw, http.StatusBadRequest, "JSON payload expected")
		return false
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(b, obj)
	if err != nil {
		panic(err)
	}
	return true
}

func (s *Server) handleUpdateTransaction(rw http.ResponseWriter, r *http.Request) {
	db := s.getDB(r)
	rs := s.getRS(r)
	tid, ok := extractTransactionID(rw, r, rs)
	if !ok {
		return
	}
	req := &payload.UpdateTransactionRequest{}
	if !parseJSONRequest(r, rw, req) {
		return
	}
	if req.Commit {
		if err := transaction.Commit(db, rs, *tid); err != nil {
			panic(err)
		}
	} else if req.Discard {
		if err := transaction.Discard(rs, *tid); err != nil {
			panic(err)
		}
	} else {
		SendError(rw, http.StatusBadRequest, "must either discard or commit transaction")
		return
	}
}
