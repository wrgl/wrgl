package authoauth2

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func request(t *testing.T, method, path, contentType, content string) *http.Response {
	req, err := http.NewRequest(method, path, bytes.NewReader([]byte(content)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)
	client := noRedirectClient()
	resp, err := client.Do(req)
	require.NoError(t, err)
	return resp
}

type responseTestCase struct {
	Resp    *http.Response
	Status  int
	Payload interface{}
}

func assertJSONResponse(t *testing.T, i int, c responseTestCase) {
	t.Helper()
	assert.Equal(t, c.Status, c.Resp.StatusCode, "case %d", i)
	b, err := ioutil.ReadAll(c.Resp.Body)
	require.NoError(t, err, "case %d", i)
	require.NoError(t, c.Resp.Body.Close(), "case %d", i)
	if c.Payload == nil {
		assert.Len(t, b, 0, "case %d", i)
	} else {
		require.Contains(t, c.Resp.Header.Get("Content-Type"), "application/json", "case %d", i)
		pt := reflect.TypeOf(c.Payload)
		if pt.Kind() == reflect.Ptr {
			pt = pt.Elem()
		}
		obj := reflect.New(pt).Interface()
		require.NoError(t, json.Unmarshal(b, obj), "case %d", i)
		assert.Equal(t, c.Payload, obj, "case %d", i)
	}
}

func TestAuthorizeEndpoint(t *testing.T) {
	url, _, _, _, stop := startHandler(t, config)
	defer stop()

	endpoint := url + "/oauth2/authorize/"
	for i, c := range []responseTestCase{
		{
			Resp:    request(t, http.MethodPut, endpoint, "", ""),
			Status:  http.StatusMethodNotAllowed,
			Payload: &HTTPError{Message: "method not allowed"},
		},
		{
			Resp:    request(t, http.MethodPost, endpoint, "application/json", ""),
			Status:  http.StatusBadRequest,
			Payload: &HTTPError{Message: `unsupported content type "application/json"`},
		},
		{
			Resp:    postForm(t, endpoint, map[string][]string{}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_request", "response_type must be code"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"response_type": {"code"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_client", "unknown client"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"response_type": {"code"},
				"client_id":     {"abcxyz"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_client", "unknown client"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"response_type": {"code"},
				"client_id":     {"wrglhub"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_request", "code_challenge required"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"response_type":  {"code"},
				"client_id":      {"wrglhub"},
				"code_challenge": {"abcxyz"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_request", "state required"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"response_type":  {"code"},
				"client_id":      {"wrglhub"},
				"code_challenge": {"abcxyz"},
				"state":          {"123456"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_request", "redirect_uri required"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"response_type":  {"code"},
				"client_id":      {"wrglhub"},
				"code_challenge": {"abcxyz"},
				"state":          {"123456"},
				"redirect_uri":   {"http://hub.wrgl.co/@john/r/data/refs/heads/main"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_request", "code_challenge_method must be S256"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"response_type":         {"code"},
				"client_id":             {"wrglhub"},
				"code_challenge":        {"abcxyz"},
				"state":                 {"123456"},
				"redirect_uri":          {"http://hub.wrgl.co/@dave/r/data/refs/heads/main"},
				"code_challenge_method": {"S256"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_request", "invalid redirect_uri"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"response_type":         {"code"},
				"client_id":             {"wrglhub"},
				"code_challenge":        {"abcxyz"},
				"state":                 {"123456"},
				"redirect_uri":          {"http://hub.wrgl.co/@john/r/data/refs/heads/main"},
				"code_challenge_method": {"S256"},
			}),
			Status: http.StatusFound,
		},
	} {
		assertJSONResponse(t, i, c)
	}
}
