package main

import (
	"encoding/hex"
	"log"
	"net/http"
	"sort"
	"strings"

	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/versioning"
)

type Server struct {
	serveMux *http.ServeMux
	db       kv.Store
	fs       kv.FileStore
}

func NewServer(db kv.Store, fs kv.FileStore) *Server {
	s := &Server{
		serveMux: http.NewServeMux(),
		db:       db,
		fs:       fs,
	}
	s.serveMux.HandleFunc("/info/refs/", s.logging("GET", s.HandleInfoRefs))
	return s
}

func (*Server) logging(method string, f func(rw http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.RequestURI())
		if r.Method != method {
			http.Error(rw, "forbidden", http.StatusForbidden)
		}
		err := f(rw, r)
		if err != nil {
			log.Printf("Error: %v", err)
			http.Error(rw, "internal server error", http.StatusInternalServerError)
		}
	}
}

func (s *Server) HandleInfoRefs(rw http.ResponseWriter, r *http.Request) error {
	refs, err := versioning.ListAllRefs(s.db)
	if err != nil {
		return err
	}
	pairs := make([][]string, 0, len(refs))
	for k, v := range refs {
		pairs = append(pairs, []string{
			hex.EncodeToString(v), k,
		})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i][1] < pairs[j][1]
	})
	buf := misc.NewBuffer(nil)
	for _, sl := range pairs {
		err := encoding.WritePktLine(rw, buf, strings.Join(sl, " "))
		if err != nil {
			return err
		}
	}
	err = encoding.WritePktLine(rw, buf, "")
	if err != nil {
		return err
	}
	return nil
}
