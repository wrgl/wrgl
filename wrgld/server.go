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
	s.serveMux.HandleFunc(api.PathReceivePack, logging(api.NewGetRefsHandler(rs)))
	s.serveMux.HandleFunc(api.PathUploadPack, logging(api.NewUploadPackHandler(db, rs, 0)))
	s.serveMux.HandleFunc(api.PathReceivePack, logging(api.NewReceivePackHandler(db, rs, c)))
	s.serveMux.HandleFunc(api.PathCommit, logging(api.NewCommitHandler(db, rs)))
	return s
}
