package server

import (
	"net/http"

	"github.com/wrgl/wrgl/pkg/api/payload"
)

func (s *Server) handleCreateTransaction(rw http.ResponseWriter, r *http.Request) {
	rs := s.getRS(r)
	id, err := rs.NewTransaction()
	if err != nil {
		panic(err)
	}
	WriteJSON(rw, &payload.CreateTransactionResponse{
		ID: id.String(),
	})
}
