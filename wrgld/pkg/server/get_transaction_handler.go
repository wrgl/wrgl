package server

import (
	"encoding/hex"
	"net/http"
	"regexp"
	"sort"

	"github.com/google/uuid"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/transaction"
)

var transactionURIPat = regexp.MustCompile(`/transactions/([0-9a-f-]+)/`)

func extractTransactionID(rw http.ResponseWriter, r *http.Request, rs ref.Store) (*uuid.UUID, bool) {
	m := transactionURIPat.FindStringSubmatch(r.URL.Path)
	if m == nil {
		SendHTTPError(rw, http.StatusNotFound)
		return nil, false
	}
	tid, err := uuid.Parse(m[1])
	if err != nil {
		SendError(rw, http.StatusBadRequest, "invalid transaction id")
		return nil, false
	}
	return &tid, true
}

func (s *Server) handleGetTransaction(rw http.ResponseWriter, r *http.Request) {
	rs := s.getRS(r)
	tid, ok := extractTransactionID(rw, r, rs)
	if !ok {
		return
	}
	refs, tx, err := transaction.Diff(rs, *tid)
	if err != nil {
		if err == ref.ErrKeyNotFound {
			SendError(rw, http.StatusNotFound, "transaction not found")
			return
		}
		panic(err)
	}
	resp := &payload.GetTransactionResponse{
		Begin:  tx.Begin,
		Status: string(tx.Status),
		End:    tx.End,
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
