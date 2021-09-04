// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"context"
	"log"
	"net/http"
	"time"

	apiserver "github.com/wrgl/core/pkg/api/server"
	"github.com/wrgl/core/pkg/auth"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

type Server struct {
	srv *http.Server
	db  objects.Store
	s   *apiserver.Server
}

func NewServer(authnS auth.AuthnStore, authzS auth.AuthzStore, db objects.Store, rs ref.Store, c conf.Store, readTimeout, writeTimeout time.Duration) *Server {
	upSessions := apiserver.NewUploadPackSessionMap()
	rpSessions := apiserver.NewReceivePackSessionMap()
	s := &Server{
		srv: &http.Server{
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
		},
		db: db,
		s: apiserver.NewServer(
			func(repo string) auth.AuthnStore { return authnS },
			func(repo string) auth.AuthzStore { return authzS },
			func(repo string) objects.Store { return db },
			func(repo string) ref.Store { return rs },
			func(repo string) conf.Store { return c },
			func(repo string) apiserver.UploadPackSessionStore { return upSessions },
			func(repo string) apiserver.ReceivePackSessionStore { return rpSessions },
		),
	}
	s.srv.Handler = RecoveryMiddleware(LoggingMiddleware(s.s))
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
