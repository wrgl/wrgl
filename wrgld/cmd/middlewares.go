// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgld

import (
	"log"
	"net/http"
	"runtime/debug"
	"time"
)

type loggingMiddleware struct {
	handler http.Handler
}

func (h *loggingMiddleware) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.handler.ServeHTTP(rw, r)
	log.Printf("%s %s (%s)", r.Method, r.URL.RequestURI(), time.Since(start))
}

func LoggingMiddleware(handler http.Handler) http.Handler {
	return &loggingMiddleware{handler: handler}
}

type recoveryMiddleware struct {
	handler http.Handler
}

func (h *recoveryMiddleware) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic (recovered): %v %s", r, string(debug.Stack()))
			http.Error(rw, "internal server error", http.StatusInternalServerError)
		}
	}()
	h.handler.ServeHTTP(rw, r)
}

func RecoveryMiddleware(handler http.Handler) http.Handler {
	return &recoveryMiddleware{handler: handler}
}
