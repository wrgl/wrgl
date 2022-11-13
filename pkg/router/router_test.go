// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package router

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func mockHandler(msg string) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		rw.Write([]byte(msg))
	}
}

func assertResponse(t *testing.T, handler http.Handler, method, path string, status int, resp string) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, status, rec.Result().StatusCode)
	b, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)
	assert.Equal(t, resp, string(b))
}

func assertRedirect(t *testing.T, handler http.Handler, method, path string, status int, newPath string) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, status, rec.Result().StatusCode)
	assert.Equal(t, newPath, rec.Result().Header.Get("Location"))
}

func TestRouter(t *testing.T) {
	for _, root := range []string{`^/my-root/`, `^/my-root`} {
		router := NewRouter(regexp.MustCompile(root), &Routes{
			Subs: []*Routes{
				{http.MethodGet, regexp.MustCompile(`^/refs/`), mockHandler("get refs"), nil},
				{http.MethodPost, regexp.MustCompile(`^/upload-pack/`), mockHandler("upload pack"), nil},
				{http.MethodGet, regexp.MustCompile(`^/diff/[0-9a-z]{32}/[0-9a-z]{32}/`), mockHandler("diff"), nil},
				{"", regexp.MustCompile(`^/commits/`), nil, []*Routes{
					{http.MethodPost, nil, mockHandler("commit"), nil},
					{http.MethodGet, regexp.MustCompile(`^[0-9a-z]{32}/`), mockHandler("get commit"), nil},
				}},
				{"", regexp.MustCompile(`^/tables/`), nil, []*Routes{
					{http.MethodGet, regexp.MustCompile(`^[0-9a-z]{32}/`), mockHandler("get table"), []*Routes{
						{http.MethodGet, regexp.MustCompile(`^blocks/`), mockHandler("get blocks"), nil},
					}},
				}},
			},
		})

		assertResponse(t, router, http.MethodPost, "/", 404, "404 page not found\n")
		assertResponse(t, router, http.MethodPost, "/refs/", 404, "404 page not found\n")
		assertResponse(t, router, http.MethodPost, "/my-root/", 404, "404 page not found\n")
		assertResponse(t, router, http.MethodGet, "/my-root/refs/", 200, "get refs")
		assertResponse(t, router, http.MethodPost, "/my-root/refs/", 404, "404 page not found\n")
		assertResponse(t, router, http.MethodPost, "/my-root/upload-pack/", 200, "upload pack")
		assertResponse(t, router, http.MethodGet, fmt.Sprintf(
			"/my-root/diff/%x/%x/", testutils.SecureRandomBytes(16), testutils.SecureRandomBytes(16),
		), 200, "diff")
		assertResponse(t, router, http.MethodPost, "/my-root/commits/", 200, "commit")
		assertResponse(t, router, http.MethodGet, fmt.Sprintf("/my-root/commits/%x/", testutils.SecureRandomBytes(16)), 200, "get commit")
		assertResponse(t, router, http.MethodGet, fmt.Sprintf("/my-root/tables/%x/", testutils.SecureRandomBytes(16)), 200, "get table")
		assertResponse(t, router, http.MethodGet, fmt.Sprintf("/my-root/tables/%x/blocks/", testutils.SecureRandomBytes(16)), 200, "get blocks")

		// test redirect
		p := fmt.Sprintf("/my-root/tables/%x/blocks", testutils.SecureRandomBytes(16))
		assertRedirect(t, router, http.MethodGet, p, 301, p+"/")
	}
}

func TestRouterWithPrefix(t *testing.T) {
	router := NewRouter(nil, &Routes{
		Pat: regexp.MustCompile(`^/repos/[0-9a-f]{32}/`),
		Subs: []*Routes{
			{http.MethodGet, regexp.MustCompile(`^refs/`), mockHandler("get refs"), nil},
		},
	})
	assertResponse(t, router, http.MethodGet, fmt.Sprintf("/repos/%x/refs/", testutils.SecureRandomBytes(16)), 200, "get refs")
	assertResponse(t, router, http.MethodGet, "/refs/", 404, "404 page not found\n")
}
