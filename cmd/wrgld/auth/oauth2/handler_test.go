package authoauth2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/auth"
	"github.com/wrgl/wrgl/pkg/conf"
	"github.com/wrgl/wrgl/pkg/testutils"
)

var config = &conf.Config{
	Auth: &conf.Auth{
		Type: conf.ATOauth2,
		OAuth2: &conf.AuthOAuth2{
			Clients: []conf.AuthClient{
				{
					ID: "wrglhub",
					RedirectURIs: []string{
						"http://hub.wrgl.co/@john/r/data",
						"http://hub.wrgl.co/@john/r/data/*",
					},
				},
			},
		},
	},
}

type WRGLDHandler struct {
	claims *Claims
}

func (h *WRGLDHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	h.claims = getClaims(r)
	rw.Write([]byte("OK"))
}

type TestServer struct {
	handler http.Handler
}

func (s *TestServer) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(rw, r)
}

func startHandler(t *testing.T, c *conf.Config) (url string, wrgldHandler *WRGLDHandler, provider *mockOIDCProvider, sessions *SessionManager, stop func()) {
	t.Helper()
	wrgldHandler = &WRGLDHandler{}
	ts := &TestServer{}
	server := httptest.NewServer(ts)
	c.Auth.OAuth2.OIDCProvider = &conf.AuthOIDCProvider{}
	c.Auth.OAuth2.OIDCProvider.Address = server.URL
	provider = newMockOIDCProvider()
	handler, err := NewHandler(wrgldHandler, c, provider)
	require.NoError(t, err)
	ts.handler = handler
	return server.URL, wrgldHandler, provider, handler.sessions, func() {
		server.Close()
	}
}

func noRedirectClient() *http.Client {
	client := &http.Client{}
	*client = *http.DefaultClient
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return client
}

func postForm(t *testing.T, path string, values url.Values) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, path, bytes.NewReader([]byte(values.Encode())))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := noRedirectClient()
	res, err := client.Do(req)
	require.NoError(t, err)
	return res
}

func getForm(t *testing.T, path string, values url.Values) *http.Response {
	t.Helper()
	if values != nil {
		path = fmt.Sprintf("%s?%s", path, values.Encode())
	}
	req, err := http.NewRequest(http.MethodGet, path, nil)
	require.NoError(t, err)
	client := noRedirectClient()
	res, err := client.Do(req)
	require.NoError(t, err)
	return res
}

func getWithAuth(t *testing.T, path, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	client := noRedirectClient()
	res, err := client.Do(req)
	require.NoError(t, err)
	return res
}

func assertStatus(t *testing.T, resp *http.Response, status int) {
	t.Helper()
	if resp.StatusCode != status {
		b, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		resp.Body.Close()
		t.Errorf("Status code not equal:\nexpected: %d\nactual: %d\nresponse was: %s", status, resp.StatusCode, string(b))
	}
}

func assertRedirect(t *testing.T, resp *http.Response) (location *url.URL) {
	t.Helper()
	assertStatus(t, resp, http.StatusFound)
	location, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	return location
}

func decodeJSON(t *testing.T, resp *http.Response, obj interface{}) {
	t.Helper()
	assertStatus(t, resp, http.StatusOK)
	assert.True(t, strings.Contains(resp.Header.Get("Content-Type"), "application/json"))
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, obj))
	require.NoError(t, resp.Body.Close())
}

func startCodeFlow(t *testing.T, clientID, url, redirectURI string) (clientState, codeVerifier string, authURL *url.URL) {
	t.Helper()
	clientState = testutils.BrokenRandomLowerAlphaString(10)
	codeVerifier = testutils.BrokenRandomLowerAlphaString(10)
	codeChallenge := generateCodeChallenge(codeVerifier)
	resp := getForm(t, url+"/oauth2/authorize/", map[string][]string{
		"response_type":         {"code"},
		"client_id":             {"wrglhub"},
		"code_challenge":        {codeChallenge},
		"code_challenge_method": {"S256"},
		"state":                 {clientState},
		"redirect_uri":          {redirectURI},
	})
	authURL = assertRedirect(t, resp)
	return
}

func TestCodeFlow(t *testing.T) {
	url, wrgldHandler, provider, _, stop := startHandler(t, config)
	defer stop()

	redirectURI := "http://hub.wrgl.co/@john/r/data"
	clientState, codeVerifier, authURL := startCodeFlow(t, "wrglhub", url, redirectURI)

	claims := &Claims{
		Email: testutils.RandomEmail(),
		Name:  testutils.BrokenRandomLowerAlphaString(10),
		Roles: []string{auth.ScopeRepoRead, auth.ScopeRepoWrite},
	}
	code := provider.PrepareCode(claims)
	resp := getForm(t, url+"/oidc/callback/", map[string][]string{
		"state": {authURL.Query().Get("state")},
		"code":  {code},
	})
	location := assertRedirect(t, resp)
	assert.Equal(t, redirectURI, fmt.Sprintf("%s://%s%s", location.Scheme, location.Host, location.Path))
	assert.Equal(t, clientState, location.Query().Get("state"))
	assert.Equal(t, code, location.Query().Get("code"))

	resp = postForm(t, url+"/oauth2/token/", map[string][]string{
		"client_id":     {"wrglhub"},
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {redirectURI},
		"code_verifier": {codeVerifier},
	})
	tr := &TokenResponse{}
	decodeJSON(t, resp, tr)
	require.Equal(t, "Bearer", tr.TokenType)
	require.NotEmpty(t, tr.AccessToken)

	resp = getWithAuth(t, url+"/commits/abcxyz/", tr.AccessToken)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, claims, wrgldHandler.claims)
}

func TestDeviceCodeFlow(t *testing.T) {
	url, wrgldHandler, provider, _, stop := startHandler(t, &conf.Config{
		Auth: &conf.Auth{
			Type: conf.ATOauth2,
			OAuth2: &conf.AuthOAuth2{
				Clients: []conf.AuthClient{
					{
						ID: "wrgl",
					},
				},
			},
		},
	})
	defer stop()

	resp := postForm(t, url+"/oauth2/devicecode/", map[string][]string{
		"client_id": {"wrgl"},
	})
	dcr := &DeviceCodeResponse{}
	decodeJSON(t, resp, dcr)

	resp = getForm(t, dcr.VerificationURI, nil)
	assertStatus(t, resp, http.StatusOK)
	assert.True(t, strings.Contains(resp.Header.Get("Content-Type"), "text/html"))

	resp = postForm(t, dcr.VerificationURI, map[string][]string{
		"user_code": {dcr.UserCode},
	})
	location := assertRedirect(t, resp)

	claims := &Claims{
		Email: testutils.RandomEmail(),
		Name:  testutils.BrokenRandomLowerAlphaString(10),
		Roles: []string{auth.ScopeRepoRead, auth.ScopeRepoWrite},
	}
	code := provider.PrepareCode(claims)
	resp = getForm(t, url+"/oidc/callback/", map[string][]string{
		"state": {location.Query().Get("state")},
		"code":  {code},
	})
	assertStatus(t, resp, http.StatusOK)
	assert.True(t, strings.Contains(resp.Header.Get("Content-Type"), "text/html"))

	resp = postForm(t, url+"/oauth2/token/", map[string][]string{
		"client_id":   {"wrgl"},
		"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
		"device_code": {dcr.DeviceCode},
	})
	tr := &TokenResponse{}
	decodeJSON(t, resp, tr)
	require.Equal(t, "Bearer", tr.TokenType)
	require.NotEmpty(t, tr.AccessToken)

	resp = getWithAuth(t, url+"/commits/abcxyz/", tr.AccessToken)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, claims, wrgldHandler.claims)
}
