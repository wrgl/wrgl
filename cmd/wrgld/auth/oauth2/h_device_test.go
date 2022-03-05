package authoauth2

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
)

func TestDeviceEndpoint(t *testing.T) {
	url, _, _, _, stop := startHandler(t, config)
	defer stop()

	endpoint := url + "/oauth2/device/"
	for i, c := range []responseTestCase{
		{
			Resp:    request(t, http.MethodPut, endpoint, "", ""),
			Status:  http.StatusMethodNotAllowed,
			Payload: &errorTmplData{ErrorMessage: "method not allowed"},
		},
		{
			Resp:    request(t, http.MethodPost, endpoint, "application/json", ""),
			Status:  http.StatusBadRequest,
			Payload: &errorTmplData{ErrorMessage: `unsupported content type &#34;application/json&#34;`},
		},
		{
			Resp:   postForm(t, endpoint, nil),
			Status: http.StatusBadRequest,
			Payload: &deviceTmplData{
				ErrorMessage: "Invalid User Code",
			},
		},
		{
			Resp: postForm(t, endpoint, map[string][]string{
				"user_code": {uuid.New().String()},
			}),
			Status: http.StatusBadRequest,
			Payload: &deviceTmplData{
				ErrorMessage: "User Code not found",
			},
		},
	} {
		assertHTMLResponse(t, i, c)
	}
}
