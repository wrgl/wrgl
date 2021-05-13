package main

import (
	"net/http"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/pack"
)

type Server struct {
	serveMux *http.ServeMux
}

func NewServer(db kv.Store, fs kv.FileStore) *Server {
	s := &Server{
		serveMux: http.NewServeMux(),
	}
	s.serveMux.HandleFunc("/info/refs/", logging(pack.NewInfoRefsHandler(db)))
	s.serveMux.HandleFunc("/upload-pack/", logging(pack.NewUploadPackHandler(db, fs)))
	return s
}
