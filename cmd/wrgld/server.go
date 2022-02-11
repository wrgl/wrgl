// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgld

import (
	"context"
	"log"
	"net/http"
	"time"

	authlocal "github.com/wrgl/wrgl/cmd/wrgld/auth/local"
	wrgldutils "github.com/wrgl/wrgl/cmd/wrgld/utils"
	apiserver "github.com/wrgl/wrgl/pkg/api/server"
	"github.com/wrgl/wrgl/pkg/auth"
	"github.com/wrgl/wrgl/pkg/conf"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

type Server struct {
	srv *http.Server
	db  objects.Store
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
	}
	handler := apiserver.NewServer(
		nil,
		func(r *http.Request) objects.Store { return db },
		func(r *http.Request) ref.Store { return rs },
		func(r *http.Request) conf.Store { return c },
		func(r *http.Request) apiserver.UploadPackSessionStore { return upSessions },
		func(r *http.Request) apiserver.ReceivePackSessionStore { return rpSessions },
	)
	s.srv.Handler = wrgldutils.ApplyMiddlewares(
		authlocal.NewHandler(
			handler, nil, authnS, authzS, false,
		),
		LoggingMiddleware,
		RecoveryMiddleware,
	)
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
