package authoauth2

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertHTMLContains(t *testing.T, html, text string, i ...interface{}) {
	t.Helper()
	msg := ""
	if len(i) > 0 {
		msg = fmt.Sprintf("\n"+i[0].(string), i[1:]...)
	}
	if !strings.Contains(html, text) {
		t.Errorf("HTML does not contain %q\nFull HTML:\n%s%s", text, html, msg)
	}
}

func assertHTMLResponse(t *testing.T, i int, c responseTestCase) {
	t.Helper()
	assert.Equal(t, c.Status, c.Resp.StatusCode, "case %d", i)
	b, err := ioutil.ReadAll(c.Resp.Body)
	require.NoError(t, err, "case %d", i)
	require.NoError(t, c.Resp.Body.Close(), "case %d", i)
	if c.Payload == nil {
		assert.Len(t, b, 0, "case %d", i)
	} else {
		require.Contains(t, c.Resp.Header.Get("Content-Type"), "text/html", "case %d", i)
		switch v := c.Payload.(type) {
		case *errorTmplData:
			assertHTMLContains(t, string(b), fmt.Sprintf(`<h2 class="error">Error: %s</h2>`, v.ErrorMessage), "case %d", i)
		case *deviceLoggedInTmplData:
			assertHTMLContains(t, string(b), "<h2>Device logged in!</h2>", "case %d", i)
		case *deviceTmplData:
			assertHTMLContains(t, string(b), fmt.Sprintf(`<p class="error">%s</p>`, v.ErrorMessage), "case %d", i)
		default:
			t.Errorf("unsupported type %T (case %d)", v, i)
		}
	}
}

func TestCallbackEndpoint(t *testing.T) {
	url, _, _, _, stop := startHandler(t, config)
	defer stop()

	redirectURI := "http://hub.wrgl.co/@john/r/data"
	_, _, authURL := startCodeFlow(t, "wrglhub", url, redirectURI)

	endpoint := url + "/oidc/callback/"
	for i, c := range []responseTestCase{
		{
			Resp:    request(t, http.MethodPost, endpoint, "", ""),
			Status:  http.StatusMethodNotAllowed,
			Payload: &errorTmplData{ErrorMessage: "method not allowed"},
		},
		{
			Resp:    getForm(t, endpoint, nil),
			Status:  http.StatusBadRequest,
			Payload: &errorTmplData{ErrorMessage: "state is missing"},
		},
		{
			Resp: getForm(t, endpoint, map[string][]string{
				"state": {"abcdef"},
			}),
			Status:  http.StatusBadRequest,
			Payload: &errorTmplData{ErrorMessage: "invalid state"},
		},
		{
			Resp: getForm(t, endpoint, map[string][]string{
				"state": {authURL.Query().Get("state")},
			}),
			Status:  http.StatusBadRequest,
			Payload: &errorTmplData{ErrorMessage: "code is missing"},
		},
	} {
		assertHTMLResponse(t, i, c)
	}
}
