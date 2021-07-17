// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/testutils"
)

type mockHandler struct {
	msg string
}

func (h *mockHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	rw.Write([]byte(h.msg))
}

func assertResponse(t *testing.T, handler http.Handler, method, path string, status int, resp string) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, status, rec.Result().StatusCode)
	b, err := ioutil.ReadAll(rec.Result().Body)
	require.NoError(t, err)
	assert.Equal(t, resp, string(b))
}

func TestRouter(t *testing.T) {
	router := NewRouter(&Routes{
		Subs: []*Routes{
			{http.MethodGet, regexp.MustCompile(`^/refs/`), &mockHandler{"get refs"}, nil},
			{http.MethodPost, regexp.MustCompile(`^/upload-pack/`), &mockHandler{"upload pack"}, nil},
			{http.MethodGet, regexp.MustCompile(`^/diff/[0-9a-z]{32}/[0-9a-z]{32}/`), &mockHandler{"diff"}, nil},
			{"", regexp.MustCompile(`^/commits/`), nil, []*Routes{
				{http.MethodPost, nil, &mockHandler{"commit"}, nil},
				{http.MethodGet, regexp.MustCompile(`^[0-9a-z]{32}/`), &mockHandler{"get commit"}, nil},
			}},
			{"", regexp.MustCompile(`^/tables/`), nil, []*Routes{
				{http.MethodGet, regexp.MustCompile(`^[0-9a-z]{32}/`), &mockHandler{"get table"}, []*Routes{
					{http.MethodGet, regexp.MustCompile(`^blocks/`), &mockHandler{"get blocks"}, nil},
				}},
			}},
		},
	})

	assertResponse(t, router, http.MethodPost, "/", 404, "404 page not found\n")
	assertResponse(t, router, http.MethodGet, "/refs/", 200, "get refs")
	assertResponse(t, router, http.MethodPost, "/refs/", 404, "404 page not found\n")
	assertResponse(t, router, http.MethodPost, "/upload-pack/", 200, "upload pack")
	assertResponse(t, router, http.MethodGet, fmt.Sprintf(
		"/diff/%x/%x/", testutils.SecureRandomBytes(16), testutils.SecureRandomBytes(16),
	), 200, "diff")
	assertResponse(t, router, http.MethodPost, "/commits/", 200, "commit")
	assertResponse(t, router, http.MethodGet, fmt.Sprintf("/commits/%x/", testutils.SecureRandomBytes(16)), 200, "get commit")
	assertResponse(t, router, http.MethodGet, fmt.Sprintf("/tables/%x/", testutils.SecureRandomBytes(16)), 200, "get table")
	assertResponse(t, router, http.MethodGet, fmt.Sprintf("/tables/%x/blocks/", testutils.SecureRandomBytes(16)), 200, "get blocks")
}
