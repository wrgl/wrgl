package server

import (
	"encoding/hex"
	"net/http"
	"regexp"
	"sort"

	"github.com/google/uuid"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/transaction"
)

var transactionURIPat = regexp.MustCompile(`/transactions/([0-9a-f-]+)/`)

func extractTransactionID(rw http.ResponseWriter, r *http.Request, db objects.Store) (*uuid.UUID, *objects.Transaction, bool) {
	m := transactionURIPat.FindStringSubmatch(r.URL.Path)
	if m == nil {
		SendHTTPError(rw, http.StatusNotFound)
		return nil, nil, false
	}
	tid, err := uuid.Parse(m[1])
	if err != nil {
		SendError(rw, http.StatusBadRequest, "invalid transaction id")
		return nil, nil, false
	}
	tx, err := objects.GetTransaction(db, tid)
	if err != nil {
		SendError(rw, http.StatusNotFound, "transaction not found")
		return nil, nil, false
	}
	return &tid, tx, true
}

func (s *Server) handleGetTransaction(rw http.ResponseWriter, r *http.Request) {
	db := s.getDB(r)
	tid, tx, ok := extractTransactionID(rw, r, db)
	if !ok {
		return
	}
	rs := s.getRS(r)
	refs, err := transaction.Diff(rs, *tid)
	if err != nil {
		panic(err)
	}
	resp := &payload.GetTransactionResponse{
		Begin: tx.Begin,
	}
	for branch, sums := range refs {
		if sums[1] != nil {
			resp.Branches = append(resp.Branches, payload.TxBranch{
				Name:       branch,
				CurrentSum: hex.EncodeToString(sums[1]),
				NewSum:     hex.EncodeToString(sums[0]),
			})
		} else {
			resp.Branches = append(resp.Branches, payload.TxBranch{
				Name:   branch,
				NewSum: hex.EncodeToString(sums[0]),
			})
		}
	}
	sort.Slice(resp.Branches, func(i, j int) bool {
		return resp.Branches[i].Name < resp.Branches[j].Name
	})
	WriteJSON(rw, resp)
}
