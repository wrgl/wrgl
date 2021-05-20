// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"net/http"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/pack"
	"github.com/wrgl/core/pkg/versioning"
)

type Server struct {
	serveMux *http.ServeMux
}

func NewServer(db kv.Store, fs kv.FileStore, c *versioning.Config) *Server {
	s := &Server{
		serveMux: http.NewServeMux(),
	}
	s.serveMux.HandleFunc("/info/refs/", logging(pack.NewInfoRefsHandler(db)))
	s.serveMux.HandleFunc("/upload-pack/", logging(pack.NewUploadPackHandler(db, fs)))
	s.serveMux.HandleFunc("/receive-pack/", logging(pack.NewReceivePackHandler(db, fs, c)))
	return s
}
