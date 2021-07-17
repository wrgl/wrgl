// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"net/http"
	"regexp"

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
	s.serveMux.HandleFunc("/", logging(NewRouter(&Routes{
		Subs: []*Routes{
			{http.MethodGet, regexp.MustCompile(`^/refs/`), api.NewGetRefsHandler(rs), nil},
			{http.MethodPost, regexp.MustCompile(`^/upload-pack/`), api.NewUploadPackHandler(db, rs, 0), nil},
			{http.MethodPost, regexp.MustCompile(`^/receive-pack/`), api.NewReceivePackHandler(db, rs, c), nil},
			{"", regexp.MustCompile(`^/commits/`), nil, []*Routes{
				{http.MethodPost, nil, api.NewCommitHandler(db, rs), nil},
				{http.MethodGet, regexp.MustCompile(`^[0-9a-z]{32}/`), api.NewGetCommitHandler(db), nil},
			}},
			{"", regexp.MustCompile(`^/tables/`), nil, []*Routes{
				{http.MethodGet, regexp.MustCompile(`^[0-9a-z]{32}/`), api.NewGetTableHandler(db), []*Routes{
					{http.MethodGet, regexp.MustCompile(`^blocks/`), api.NewGetBlocksHandler(db), nil},
					{http.MethodGet, regexp.MustCompile(`^rows/`), api.NewGetRowsHandler(db), nil},
				}},
			}},
			{http.MethodGet, regexp.MustCompile(`^/diff/[0-9a-z]{32}/[0-9a-z]{32}/`), api.NewDiffHandler(db), nil},
		},
	})))
	return s
}
