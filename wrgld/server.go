// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"net/http"
	"regexp"

	apiserver "github.com/wrgl/core/pkg/api/server"
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
			{http.MethodGet, regexp.MustCompile(`^/refs/`), apiserver.NewGetRefsHandler(rs), nil},
			{http.MethodPost, regexp.MustCompile(`^/upload-pack/`), apiserver.NewUploadPackHandler(db, rs, apiserver.NewUploadPackSessionMap(), 0), nil},
			{http.MethodPost, regexp.MustCompile(`^/receive-pack/`), apiserver.NewReceivePackHandler(db, rs, c, apiserver.NewReceivePackSessionMap()), nil},
			{"", regexp.MustCompile(`^/commits/`), nil, []*Routes{
				{http.MethodPost, nil, apiserver.NewCommitHandler(db, rs), nil},
				{http.MethodGet, regexp.MustCompile(`^[0-9a-z]{32}/`), apiserver.NewGetCommitHandler(db), nil},
			}},
			{"", regexp.MustCompile(`^/tables/`), nil, []*Routes{
				{http.MethodGet, regexp.MustCompile(`^[0-9a-z]{32}/`), apiserver.NewGetTableHandler(db), []*Routes{
					{http.MethodGet, regexp.MustCompile(`^blocks/`), apiserver.NewGetBlocksHandler(db), nil},
					{http.MethodGet, regexp.MustCompile(`^rows/`), apiserver.NewGetRowsHandler(db), nil},
				}},
			}},
			{http.MethodGet, regexp.MustCompile(`^/diff/[0-9a-z]{32}/[0-9a-z]{32}/`), apiserver.NewDiffHandler(db), nil},
		},
	})))
	return s
}
