package server

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/ref"
)

func (s *Server) handleCreateTransaction(rw http.ResponseWriter, r *http.Request) {
	var tx *ref.Transaction
	rs := s.getRS(r)
	if v := r.Header.Get("Content-Type"); strings.Contains(v, api.CTJSON) {
		req := &payload.CreateTransactionRequest{}
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		if err = json.Unmarshal(b, req); err != nil {
			panic(err)
		}
		id, err := uuid.Parse(req.ID)
		if err != nil {
			SendError(rw, http.StatusBadRequest, "invalid id")
			return
		}
		_, err = rs.GetTransaction(id)
		if err == nil {
			SendError(rw, http.StatusBadRequest, "transaction already created")
			return
		}
		if req.Begin.IsZero() {
			SendError(rw, http.StatusBadRequest, "begin time missing")
			return
		}
		if req.Status != string(ref.TSCommitted) && req.Status != string(ref.TSInProgress) {
			SendError(rw, http.StatusBadRequest, "invalid status")
			return
		}
		tx = &ref.Transaction{
			ID:     id,
			Begin:  req.Begin,
			Status: ref.TransactionStatus(req.Status),
			End:    req.End,
		}
	}
	id, err := rs.NewTransaction(tx)
	if err != nil {
		panic(err)
	}
	WriteJSON(rw, &payload.CreateTransactionResponse{
		ID: id.String(),
	})
}
