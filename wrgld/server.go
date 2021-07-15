// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"net/http"

	"github.com/wrgl/core/pkg/api"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

type Server struct {
	serveMux *http.ServeMux
}

func NewServer(db objects.Store, rs ref.Store, c *conf.Config) *Server {
	s := &Server{
		serveMux: http.NewServeMux(),
	}
	s.serveMux.HandleFunc("/info/refs/", logging(api.NewInfoRefsHandler(rs)))
	s.serveMux.HandleFunc("/upload-pack/", logging(api.NewUploadPackHandler(db, rs, 0)))
	s.serveMux.HandleFunc("/receive-pack/", logging(api.NewReceivePackHandler(db, rs, c)))
	return s
}
