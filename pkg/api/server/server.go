// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/wrgl/core/pkg/auth"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/router"
)

var (
	patAuthenticate *regexp.Regexp
	patConfig       *regexp.Regexp
	patRefs         *regexp.Regexp
	patHead         *regexp.Regexp
	patRefsHead     *regexp.Regexp
	patUploadPack   *regexp.Regexp
	patReceivePack  *regexp.Regexp
	patCommits      *regexp.Regexp
	patCommit       *regexp.Regexp
	patSum          *regexp.Regexp
	patTables       *regexp.Regexp
	patTable        *regexp.Regexp
	patBlocks       *regexp.Regexp
	patTableBlocks  *regexp.Regexp
	patRows         *regexp.Regexp
	patTableRows    *regexp.Regexp
	patDiff         *regexp.Regexp
)

type claimsKey struct{}

func SetClaims(r *http.Request, claims *auth.Claims) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), claimsKey{}, claims))
}

func getClaims(r *http.Request) *auth.Claims {
	if i := r.Context().Value(claimsKey{}); i != nil {
		return i.(*auth.Claims)
	}
	return nil
}

type ServerOption func(s *Server)

func WithPostCommitCallback(postCommit func(r *http.Request, commit *objects.Commit, sum []byte, branch string)) ServerOption {
	return func(s *Server) {
		s.postCommit = postCommit
	}
}

type Server struct {
	getAuthnS    func(r *http.Request) auth.AuthnStore
	getDB        func(r *http.Request) objects.Store
	getRS        func(r *http.Request) ref.Store
	getConfS     func(r *http.Request) conf.Store
	getUpSession func(r *http.Request) UploadPackSessionStore
	getRPSession func(r *http.Request) ReceivePackSessionStore
	postCommit   func(r *http.Request, commit *objects.Commit, sum []byte, branch string)
	router       *router.Router
	maxAge       time.Duration
}

func NewServer(
	rootPath *regexp.Regexp, getAuthnS func(r *http.Request) auth.AuthnStore, getDB func(r *http.Request) objects.Store, getRS func(r *http.Request) ref.Store,
	getConfS func(r *http.Request) conf.Store, getUpSession func(r *http.Request) UploadPackSessionStore, getRPSession func(r *http.Request) ReceivePackSessionStore,
	opts ...ServerOption,
) *Server {
	s := &Server{
		getAuthnS:    getAuthnS,
		getDB:        getDB,
		getRS:        getRS,
		getConfS:     getConfS,
		getUpSession: getUpSession,
		getRPSession: getRPSession,
		maxAge:       90 * 24 * time.Hour,
	}
	s.router = router.NewRouter(rootPath, &router.Routes{
		Subs: []*router.Routes{
			{
				Method:      http.MethodPost,
				Pat:         patAuthenticate,
				HandlerFunc: s.handleAuthenticate,
			},
			{
				Pat: patConfig,
				Subs: []*router.Routes{
					{
						Method:      http.MethodGet,
						HandlerFunc: s.handleGetConfig,
					},
					{
						Method:      http.MethodPut,
						HandlerFunc: s.handlePutConfig,
					},
				},
			},
			{
				Pat: patRefs,
				Subs: []*router.Routes{
					{
						Method:      http.MethodGet,
						HandlerFunc: s.handleGetRefs,
					},
					{
						Method:      http.MethodGet,
						Pat:         patHead,
						HandlerFunc: s.handleGetHead,
					},
				},
			},
			{
				Method:      http.MethodPost,
				Pat:         patUploadPack,
				HandlerFunc: s.handleUploadPack,
			},
			{
				Method:      http.MethodPost,
				Pat:         patReceivePack,
				HandlerFunc: s.handleReceivePack,
			},
			{
				Pat: patCommits,
				Subs: []*router.Routes{
					{
						Method:      http.MethodPost,
						HandlerFunc: s.handleCommit,
					},
					{
						Method:      http.MethodGet,
						HandlerFunc: s.handleGetCommits,
					},
					{
						Method:      http.MethodGet,
						Pat:         patSum,
						HandlerFunc: s.handleGetCommit,
					},
				}},
			{
				Pat: patTables,
				Subs: []*router.Routes{
					{
						Method:      http.MethodGet,
						Pat:         patSum,
						HandlerFunc: s.handleGetTable,
						Subs: []*router.Routes{
							{
								Method:      http.MethodGet,
								Pat:         patBlocks,
								HandlerFunc: s.handleGetBlocks,
							},
							{
								Method:      http.MethodGet,
								Pat:         patRows,
								HandlerFunc: s.handleGetRows,
							},
						}},
				}},
			{
				Method:      http.MethodGet,
				Pat:         patDiff,
				HandlerFunc: s.handleDiff,
			},
		},
	})
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Server) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(rw, r)
}

func (s *Server) cacheControlImmutable(rw http.ResponseWriter) {
	rw.Header().Set(
		"Cache-Control",
		fmt.Sprintf("public, immutable, max-age=%d", int(s.maxAge.Seconds())),
	)
}
