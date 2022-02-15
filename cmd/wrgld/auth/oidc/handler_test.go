package authoidc

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/gobwas/glob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/testutils"
)

type serverHandler struct{}

func (p *serverHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {}

func postFromRequest(target string, query url.Values) *http.Request {
	r := httptest.NewRequest(http.MethodPost, target, bytes.NewReader([]byte(query.Encode())))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}

func do(t *testing.T, r *http.Request) *http.Response {
	resp, err := http.DefaultClient.Do(r)
	require.NoError(t, err)
	return resp
}

func assertJSONResponse(t *testing.T, resp *http.Response, code int, payload interface{}) {
	assert.Equal(t, code, resp.StatusCode)
	assert.Contains(t, "application/json", resp.Header.Get("Content-Type"))
	ptr := reflect.New(reflect.TypeOf(payload).Elem()).Interface()
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, ptr))
	assert.Equal(t, payload, ptr)
}

func TestHandler(t *testing.T) {
	oidcClientID := testutils.BrokenRandomLowerAlphaString(6)
	oidcClientSecret := testutils.BrokenRandomLowerAlphaString(10)
	p := startOIDCProvider(oidcClientID, oidcClientSecret)
	defer p.Close()
	clientID := testutils.BrokenRandomLowerAlphaString(6)
	h, err := NewHandler(HandlerOptions{
		AcceptedClientIDs: []string{clientID},
		ValidRedirectURIs: []glob.Glob{
			glob.MustCompile(`http://localhost/redirect`),
		},
		OIDCProviderURI:  p.s.URL,
		OIDCClientID:     oidcClientID,
		OIDCClientSecret: oidcClientSecret,
		Handler:          &serverHandler{},
	})
	require.NoError(t, err)
	s := httptest.NewServer(h)
	defer s.Close()
	h.opts.Address = s.URL

	req := httptest.NewRequest(http.MethodPut, s.URL+"/oauth2/authorize/", nil)
	resp := do(t, req)
	assertJSONResponse(t, resp, http.StatusMethodNotAllowed, &HTTPError{Message: "method not allowed"})
	// req = postFromRequest(s.URL+"/oauth2/authorize/", url.Values{
	// 	"response_type": {"code"},
	// })
}
