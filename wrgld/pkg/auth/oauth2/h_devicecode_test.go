package authoauth2

import (
	"net/http"
	"testing"
)

func TestDeviceCodeEndpoint(t *testing.T) {
	url, _, _, _, stop := startHandler(t, config)
	defer stop()

	endpoint := url + "/oauth2/devicecode/"
	for i, c := range []responseTestCase{
		{
			Resp:    request(t, http.MethodGet, endpoint, "", ""),
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
			Payload: &Oauth2Error{"invalid_client", "unknown client"},
		},
	} {
		assertJSONResponse(t, i, c)
	}
}
