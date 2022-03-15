package server_testutils

import (
	"encoding/hex"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

const (
	headerRequestCaptureMiddlewareKey = "Request-Capture-Middleware-Key"
)

type Middleware func(h http.Handler) http.Handler

func ApplyMiddlewares(handler http.Handler, middlewares ...Middleware) http.Handler {
	for _, m := range middlewares {
		handler = m(handler)
	}
	return handler
}

type RequestCaptureMiddleware struct {
	handler  http.Handler
	requests map[string]*http.Request
}

func NewRequestCaptureMiddleware(handler http.Handler) *RequestCaptureMiddleware {
	return &RequestCaptureMiddleware{
		handler:  handler,
		requests: map[string]*http.Request{},
	}
}

func (m *RequestCaptureMiddleware) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if v := r.Header.Get(headerRequestCaptureMiddlewareKey); v != "" {
		m.requests[v] = &http.Request{}
		*m.requests[v] = *r
	}
	m.handler.ServeHTTP(rw, r)
}

func (m *RequestCaptureMiddleware) Capture(t *testing.T, f func(header http.Header)) *http.Request {
	t.Helper()
	k := hex.EncodeToString(testutils.SecureRandomBytes(16))
	header := http.Header{}
	header.Set(headerRequestCaptureMiddlewareKey, k)
	f(header)
	r, ok := m.requests[k]
	require.True(t, ok, "request not found for key %q", k)
	return r
}
