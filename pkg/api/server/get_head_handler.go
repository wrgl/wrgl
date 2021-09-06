package apiserver

import (
	"net/http"
	"regexp"

	"github.com/wrgl/core/pkg/ref"
)

var headURIPat = regexp.MustCompile(`/refs/heads/([^/]+)/`)

func (s *Server) handleGetHead(rw http.ResponseWriter, r *http.Request) {
	m := headURIPat.FindStringSubmatch(r.URL.Path)
	if m == nil {
		http.NotFound(rw, r)
		return
	}
	rs := s.getRS(r)
	sum, err := ref.GetHead(rs, m[1])
	if err != nil {
		http.NotFound(rw, r)
		return
	}
	db := s.getDB(r)
	writeCommitJSON(rw, r, db, sum)
}
