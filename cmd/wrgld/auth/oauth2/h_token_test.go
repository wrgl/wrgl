package authoauth2

import (
	"net/http"
	"testing"

	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestTokenEndpoint(t *testing.T) {
	url, _, _, sessions, stop := startHandler(t, config)
	defer stop()

	redirectURI := "http://hub.wrgl.co/@john/r/data"
	codeVerifier := testutils.BrokenRandomLowerAlphaString(10)
	codeChallenge := generateCodeChallenge(codeVerifier)
	ses := &Session{
		ClientID:      "wrglhub",
		RedirectURI:   redirectURI,
		CodeChallenge: codeChallenge,
	}
	code1 := testutils.BrokenRandomLowerAlphaString(10)
	sessions.Save(code1, ses)
	code2 := testutils.BrokenRandomLowerAlphaString(10)
	sessions.Save(code2, ses)
	code3 := testutils.BrokenRandomLowerAlphaString(10)
	sessions.Save(code3, ses)
	code4 := testutils.BrokenRandomLowerAlphaString(10)
	sessions.Save(code4, ses)

	endpoint := url + "/oauth2/token/"
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
			Payload: &Oauth2Error{"invalid_request", "invalid grant_type"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"grant_type": {"authorization_code"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_request", "code required"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"grant_type": {"authorization_code"},
				"code":       {"abc123"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_request", "invalid code"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"grant_type": {"authorization_code"},
				"code":       {code1},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_client", "invalid client_id"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"grant_type": {"authorization_code"},
				"code":       {code2},
				"client_id":  {"wrglhub"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_request", "redirect_uri does not match"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"grant_type":   {"authorization_code"},
				"code":         {code3},
				"client_id":    {"wrglhub"},
				"redirect_uri": {ses.RedirectURI},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_grant", "code_verifier required"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"grant_type":    {"authorization_code"},
				"code":          {code4},
				"client_id":     {"wrglhub"},
				"redirect_uri":  {ses.RedirectURI},
				"code_verifier": {"abc123"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_grant", "code challenge failed"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"grant_type": {"urn:ietf:params:oauth:grant-type:device_code"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_request", "device_code required"},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
				"device_code": {"abc123"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &Oauth2Error{"invalid_request", "invalid device_code"},
		},
	} {
		assertJSONResponse(t, i, c)
	}
}
