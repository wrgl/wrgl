// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"context"
	"net/http"
	"regexp"
	"strings"

	"github.com/wrgl/core/pkg/auth"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/router"
)

type emailKey struct{}

func setEmail(r *http.Request, email string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), emailKey{}, email))
}

func getEmail(r *http.Request) string {
	if i := r.Context().Value(emailKey{}); i != nil {
		return i.(string)
	}
	return ""
}

type ServerOption func(s *Server)

func WithPostCommitCallback(postCommit func(commit *objects.Commit, sum []byte, branch string)) ServerOption {
	return func(s *Server) {
		s.postCommit = postCommit
	}
}

type Server struct {
	getDB        func(r *http.Request) objects.Store
	getAuthnS    func(r *http.Request) auth.AuthnStore
	getAuthzS    func(r *http.Request) auth.AuthzStore
	getRS        func(r *http.Request) ref.Store
	getConfS     func(r *http.Request) conf.Store
	getUpSession func(r *http.Request) UploadPackSessionStore
	getRPSession func(r *http.Request) ReceivePackSessionStore
	postCommit   func(commit *objects.Commit, sum []byte, branch string)
	router       *router.Router
}

func NewServer(
	getAuthnS func(r *http.Request) auth.AuthnStore, getAuthzS func(r *http.Request) auth.AuthzStore, getDB func(r *http.Request) objects.Store,
	getRS func(r *http.Request) ref.Store, getConfS func(r *http.Request) conf.Store, getUpSession func(r *http.Request) UploadPackSessionStore,
	getRPSession func(r *http.Request) ReceivePackSessionStore, opts ...ServerOption,
) *Server {
	s := &Server{
		getDB:        getDB,
		getAuthnS:    getAuthnS,
		getAuthzS:    getAuthzS,
		getRS:        getRS,
		getConfS:     getConfS,
		getUpSession: getUpSession,
		getRPSession: getRPSession,
	}
	s.router = router.NewRouter(&router.Routes{
		Subs: []*router.Routes{
			{
				Method:      http.MethodPost,
				Pat:         regexp.MustCompile(`^/authenticate/`),
				HandlerFunc: s.handleAuthenticate,
			},
			{
				Pat: regexp.MustCompile(`^/config/`),
				Subs: []*router.Routes{
					{
						Method:      http.MethodGet,
						HandlerFunc: s.authenticateMiddleware(s.authorizeMiddleware(s.handleGetConfig, auth.ScopeReadConfig)),
					},
					{
						Method:      http.MethodPut,
						HandlerFunc: s.authenticateMiddleware(s.authorizeMiddleware(s.handlePutConfig, auth.ScopeWriteConfig)),
					},
				},
			},
			{
				Pat: regexp.MustCompile(`^/refs/`),
				Subs: []*router.Routes{
					{
						Method:      http.MethodGet,
						HandlerFunc: s.authenticateMiddleware(s.authorizeMiddleware(s.handleGetRefs, auth.ScopeRead)),
					},
					{
						Method:      http.MethodGet,
						Pat:         regexp.MustCompile(`^heads/[-_0-9a-zA-Z]+/`),
						HandlerFunc: s.authenticateMiddleware(s.authorizeMiddleware(s.handleGetHead, auth.ScopeRead)),
					},
				},
			},
			{
				Method:      http.MethodPost,
				Pat:         regexp.MustCompile(`^/upload-pack/`),
				HandlerFunc: s.authenticateMiddleware(s.authorizeMiddleware(s.handleUploadPack, auth.ScopeRead)),
			},
			{
				Method:      http.MethodPost,
				Pat:         regexp.MustCompile(`^/receive-pack/`),
				HandlerFunc: s.authenticateMiddleware(s.authorizeMiddleware(s.handleReceivePack, auth.ScopeWrite)),
			},
			{
				Pat: regexp.MustCompile(`^/commits/`),
				Subs: []*router.Routes{
					{
						Method:      http.MethodPost,
						HandlerFunc: s.authenticateMiddleware(s.authorizeMiddleware(s.handleCommit, auth.ScopeWrite)),
					},
					{
						Method:      http.MethodGet,
						HandlerFunc: s.authenticateMiddleware(s.authorizeMiddleware(s.handleGetCommits, auth.ScopeRead)),
					},
					{
						Method:      http.MethodGet,
						Pat:         regexp.MustCompile(`^[0-9a-f]{32}/`),
						HandlerFunc: s.authenticateMiddleware(s.authorizeMiddleware(s.handleGetCommit, auth.ScopeRead)),
					},
				}},
			{
				Pat: regexp.MustCompile(`^/tables/`),
				Subs: []*router.Routes{
					{
						Method:      http.MethodGet,
						Pat:         regexp.MustCompile(`^[0-9a-f]{32}/`),
						HandlerFunc: s.authenticateMiddleware(s.authorizeMiddleware(s.handleGetTable, auth.ScopeRead)),
						Subs: []*router.Routes{
							{
								Method:      http.MethodGet,
								Pat:         regexp.MustCompile(`^blocks/`),
								HandlerFunc: s.authenticateMiddleware(s.authorizeMiddleware(s.handleGetBlocks, auth.ScopeRead)),
							},
							{
								Method:      http.MethodGet,
								Pat:         regexp.MustCompile(`^rows/`),
								HandlerFunc: s.authenticateMiddleware(s.authorizeMiddleware(s.handleGetRows, auth.ScopeRead)),
							},
						}},
				}},
			{
				Method:      http.MethodGet,
				Pat:         regexp.MustCompile(`^/diff/[0-9a-f]{32}/[0-9a-f]{32}/`),
				HandlerFunc: s.authenticateMiddleware(s.authorizeMiddleware(s.handleDiff, auth.ScopeRead)),
			},
		},
	})
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Server) authenticateMiddleware(handle http.HandlerFunc) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if h := r.Header.Get("Authorization"); h != "" && strings.HasPrefix(h, "Bearer ") {
			authnS := s.getAuthnS(r)
			claims, err := authnS.CheckToken(h[7:])
			if err != nil {
				if strings.HasPrefix(err.Error(), "unexpected signing method: ") || err.Error() == "invalid token" {
					http.Error(rw, "invalid token", http.StatusUnauthorized)
					return
				}
				panic(err)
			}
			r = setEmail(r, claims.Email)
		}
		handle(rw, r)
	}
}

func (s *Server) authorizeMiddleware(handle http.HandlerFunc, requiredScope string) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		authzS := s.getAuthzS(r)
		email := getEmail(r)
		if ok, err := authzS.Authorized(email, requiredScope); err != nil {
			panic(err)
		} else if !ok {
			if email == "" {
				http.Error(rw, "unauthorized", http.StatusUnauthorized)
			} else {
				http.Error(rw, "forbidden", http.StatusForbidden)
			}
			return
		}
		handle(rw, r)
	}
}

func (s *Server) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(rw, r)
}
