package authlocal

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/auth"
	authfs "github.com/wrgl/wrgl/pkg/auth/fs"
	"github.com/wrgl/wrgl/pkg/conf"
	"github.com/wrgl/wrgl/pkg/local"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func request(method, path, token string) *http.Request {
	r := httptest.NewRequest(method, "http://my.site"+path, nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

func requestWithAuthCookie(path, token string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://my.site"+path, nil)
	if token != "" {
		r.AddCookie(&http.Cookie{
			Name:     "Authorization",
			Value:    token,
			Path:     path,
			HttpOnly: true,
			Expires:  time.Now().Add(time.Hour * 24),
		})
	}
	return r
}

func jsonRequest(t *testing.T, method, path string, payload interface{}) *http.Request {
	t.Helper()
	b, err := json.Marshal(payload)
	require.NoError(t, err)
	r := httptest.NewRequest(method, "http://my.site"+path, bytes.NewReader(b))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func assertResponse(t *testing.T, handler http.Handler, r *http.Request, status int, body string) {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, r)
	resp := rec.Result()
	assert.Equal(t, status, resp.StatusCode, "invalid status")
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, body, string(b))
}

func assertJSONResponse(t *testing.T, handler http.Handler, r *http.Request, payload interface{}) {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, r)
	resp := rec.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "invalid status")
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, payload))
}

func TestHandler(t *testing.T) {
	rd := local.NewRepoDir(t.TempDir(), "")
	defer rd.Close()
	authzS, err := authfs.NewAuthzStore(rd)
	require.NoError(t, err)
	defer authzS.Close()
	authnS, err := authfs.NewAuthnStore(rd, 0)
	require.NoError(t, err)
	defer authnS.Close()
	h := NewHandler(
		http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			rw.Write([]byte("OK"))
		}),
		&conf.Config{},
		authnS, authzS,
	)

	email := "john@doe.com"
	name := "John Doe"
	password := testutils.BrokenRandomLowerAlphaString(10)
	require.NoError(t, authnS.SetName(email, name))
	require.NoError(t, authnS.SetPassword(email, password))

	assertResponse(t, h,
		request(http.MethodGet, "/authenticate/", ""),
		http.StatusMethodNotAllowed, `{"message":"method not allowed"}`,
	)

	assertResponse(t, h,
		request(http.MethodPost, "/authenticate/", ""),
		http.StatusUnsupportedMediaType, `{"message":"json expected"}`,
	)

	assertResponse(t, h,
		jsonRequest(t, http.MethodPost, "/authenticate/", &AuthenticateRequest{
			Email:    email,
			Password: "abcdef",
		}),
		http.StatusUnauthorized, `{"message":"email/password invalid"}`,
	)

	authResp := &AuthenticateResponse{}
	assertJSONResponse(t, h,
		jsonRequest(t, http.MethodPost, "/authenticate/", &AuthenticateRequest{
			Email:    email,
			Password: password,
		}),
		authResp,
	)
	assert.NotEmpty(t, authResp.IDToken)

	assertResponse(t, h,
		request(http.MethodGet, "/commits/abcd1234/", ""),
		http.StatusForbidden, `{"message":"Forbidden"}`,
	)

	assertResponse(t, h,
		request(http.MethodGet, "/commits/abcd1234/", "qwer"),
		http.StatusUnauthorized, `{"message":"invalid token"}`,
	)

	assertResponse(t, h,
		request(http.MethodGet, "/commits/abcd1234/", authResp.IDToken),
		http.StatusForbidden, `{"message":"Forbidden"}`,
	)

	require.NoError(t, authzS.AddPolicy(email, auth.ScopeRepoRead))
	assertResponse(t, h,
		request(http.MethodGet, "/commits/abcd1234/", authResp.IDToken),
		http.StatusOK, "OK",
	)

	assertResponse(t, h,
		requestWithAuthCookie("/commits/abcd1234/", "Bearer abcd123"),
		http.StatusUnauthorized, `{"message":"invalid token"}`,
	)

	require.NoError(t, authzS.AddPolicy(email, auth.ScopeRepoRead))
	assertResponse(t, h,
		requestWithAuthCookie("/commits/abcd1234/", "Bearer "+authResp.IDToken),
		http.StatusOK, "OK",
	)

	require.NoError(t, authzS.AddPolicy(email, auth.ScopeRepoRead))
	assertResponse(t, h,
		requestWithAuthCookie("/commits/abcd1234/", url.QueryEscape("Bearer "+authResp.IDToken)),
		http.StatusOK, "OK",
	)

	// should allow data read when AnonymousRead is true
	h = NewHandler(
		http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			rw.Write([]byte("OK"))
		}),
		&conf.Config{
			Auth: &conf.Auth{AnonymousRead: true},
		},
		authnS, authzS,
	)
	assertResponse(t, h,
		requestWithAuthCookie("/commits/abcd1234/", ""),
		http.StatusOK, "OK",
	)
}
