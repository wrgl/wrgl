package server

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wrgl/wrgl/pkg/conf"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/router"
	"github.com/wrgl/wrgl/pkg/sorter"
)

type ServerOption func(s *Server)

func WithPostCommitCallback(postCommit PostCommitHook) ServerOption {
	return func(s *Server) {
		s.postCommit = postCommit
	}
}

func WithDebug(w io.Writer) ServerOption {
	return func(s *Server) {
		s.debugOut = w
	}
}

type ObjectsStoreGetter func(r *http.Request) objects.Store

type RefStoreGetter func(r *http.Request) ref.Store

type ConfStoreGetter func(r *http.Request) conf.Store

type UPSessionStoreGetter func(r *http.Request) UploadPackSessionStore

type RPSessionStoreGetter func(r *http.Request) ReceivePackSessionStore

type PostCommitHook func(r *http.Request, commit *objects.Commit, sum []byte, branch string, tid *uuid.UUID)

type Server struct {
	RootPath     *regexp.Regexp
	getDB        ObjectsStoreGetter
	getRS        RefStoreGetter
	getConfS     ConfStoreGetter
	getUpSession UPSessionStoreGetter
	getRPSession RPSessionStoreGetter
	postCommit   PostCommitHook
	router       *router.Router
	maxAge       time.Duration
	debugOut     io.Writer
	sPool        *sync.Pool
}

func NewServer(
	rootPath *regexp.Regexp,
	getDB ObjectsStoreGetter,
	getRS RefStoreGetter,
	getConfS ConfStoreGetter,
	getUpSession UPSessionStoreGetter,
	getRPSession RPSessionStoreGetter,
	opts ...ServerOption,
) *Server {
	s := &Server{
		RootPath:     rootPath,
		getDB:        getDB,
		getRS:        getRS,
		getConfS:     getConfS,
		getUpSession: getUpSession,
		getRPSession: getRPSession,
		maxAge:       90 * 24 * time.Hour,
		sPool: &sync.Pool{
			New: func() interface{} {
				s, err := sorter.NewSorter(8*1024*1024, nil)
				if err != nil {
					panic(err)
				}
				return s
			},
		},
	}
	s.router = router.NewRouter(rootPath, &router.Routes{
		Subs: []*router.Routes{
			{
				Pat: patTransactions,
				Subs: []*router.Routes{
					{
						Method:      http.MethodPost,
						HandlerFunc: s.handleCreateTransaction,
					},
					{
						Method:      http.MethodGet,
						Pat:         patUUID,
						HandlerFunc: s.handleGetTransaction,
					},
					{
						Method:      http.MethodPost,
						Pat:         patUUID,
						HandlerFunc: s.handleUpdateTransaction,
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
				Method:      http.MethodGet,
				Pat:         patRootedBlocks,
				HandlerFunc: s.handleGetBlocks,
			},
			{
				Method:      http.MethodGet,
				Pat:         patRootedRows,
				HandlerFunc: s.handleGetRows,
			},
			{
				Method:      http.MethodGet,
				Pat:         patObjects,
				HandlerFunc: s.handleGetObjects,
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
						Subs: []*router.Routes{
							{
								Method:      http.MethodGet,
								Pat:         patProfile,
								HandlerFunc: s.handleGetCommitProfile,
							},
						},
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
								Pat:         patProfile,
								HandlerFunc: s.handleGetTableProfile,
							},
							{
								Method:      http.MethodGet,
								Pat:         patBlocks,
								HandlerFunc: s.handleGetTableBlocks,
							},
							{
								Method:      http.MethodGet,
								Pat:         patRows,
								HandlerFunc: s.handleGetTableRows,
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
