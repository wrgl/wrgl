// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"context"
	"log"
	"net/http"
	"regexp"
	"time"

	apiserver "github.com/wrgl/core/pkg/api/server"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/router"
)

type Server struct {
	srv *http.Server
	db  objects.Store
}

func NewServer(db objects.Store, rs ref.Store, c *conf.Config, readTimeout, writeTimeout time.Duration) *Server {
	s := &Server{
		srv: &http.Server{
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
		},
		db: db,
	}
	s.srv.Handler = RecoveryMiddleware(LoggingMiddleware(router.NewRouter(&router.Routes{
		Subs: []*router.Routes{
			{
				Method:  http.MethodGet,
				Pat:     regexp.MustCompile(`^/refs/`),
				Handler: apiserver.NewGetRefsHandler(rs),
			},
			{
				Method:  http.MethodPost,
				Pat:     regexp.MustCompile(`^/upload-pack/`),
				Handler: apiserver.NewUploadPackHandler(db, rs, apiserver.NewUploadPackSessionMap(), 0),
			},
			{
				Method:  http.MethodPost,
				Pat:     regexp.MustCompile(`^/receive-pack/`),
				Handler: apiserver.NewReceivePackHandler(db, rs, c, apiserver.NewReceivePackSessionMap()),
			},
			{
				Pat: regexp.MustCompile(`^/commits/`),
				Subs: []*router.Routes{
					{
						Method:  http.MethodPost,
						Handler: apiserver.NewCommitHandler(db, rs),
					},
					{
						Method:  http.MethodGet,
						Pat:     regexp.MustCompile(`^[0-9a-f]{32}/`),
						Handler: apiserver.NewGetCommitHandler(db),
					},
				}},
			{
				Pat: regexp.MustCompile(`^/tables/`),
				Subs: []*router.Routes{
					{
						Method:  http.MethodGet,
						Pat:     regexp.MustCompile(`^[0-9a-f]{32}/`),
						Handler: apiserver.NewGetTableHandler(db),
						Subs: []*router.Routes{
							{
								Method:  http.MethodGet,
								Pat:     regexp.MustCompile(`^blocks/`),
								Handler: apiserver.NewGetBlocksHandler(db),
							},
							{
								Method:  http.MethodGet,
								Pat:     regexp.MustCompile(`^rows/`),
								Handler: apiserver.NewGetRowsHandler(db),
							},
						}},
				}},
			{
				Method:  http.MethodGet,
				Pat:     regexp.MustCompile(`^/diff/[0-9a-f]{32}/[0-9a-f]{32}/`),
				Handler: apiserver.NewDiffHandler(db),
			},
		},
	})))
	return s
}

func (s *Server) Start(addr string) error {
	s.srv.Addr = addr
	log.Printf("server started at %s", addr)
	return s.srv.ListenAndServe()
}

func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.srv.Shutdown(ctx); err != nil {
		return err
	}
	return s.db.Close()
}
