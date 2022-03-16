package server

import (
	"net/http"

	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/transaction"
)

func (s *Server) handleCreateTransaction(rw http.ResponseWriter, r *http.Request) {
	db := s.getDB(r)
	id, err := transaction.New(db)
	if err != nil {
		panic(err)
	}
	WriteJSON(rw, &payload.CreateTransactionResponse{
		ID: id.String(),
	})
}
