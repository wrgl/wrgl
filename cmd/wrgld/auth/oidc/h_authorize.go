package authoidc

import (
	"fmt"
	"net/http"
	"net/url"
)

func (h *Handler) handleAuthorize(rw http.ResponseWriter, r *http.Request) {
	values, err := h.parseForm(r)
	if err != nil {
		handleError(rw, err)
		return
	}
	if s := values.Get("response_type"); s != "code" {
		handleError(rw, &Oauth2Error{"invalid_request", "response_type must be code"})
		return
	}
	clientID := values.Get("client_id")
	if !h.validClientID(clientID) {
		handleError(rw, &Oauth2Error{"invalid_client", "unknown client"})
		return
	}
	for _, key := range []string{
		"code_challenge",
		"state",
		"redirect_uri",
	} {
		if s := values.Get(key); s == "" {
			handleError(rw, &Oauth2Error{"invalid_request", fmt.Sprintf("%s required", key)})
			return
		}
	}
	if s := values.Get("code_challenge_method"); s != "S256" {
		handleError(rw, &Oauth2Error{"invalid_request", "code_challenge_method must be S256"})
		return
	}
	redirectURI := values.Get("redirect_uri")
	if !h.validRedirectURI(clientID, redirectURI) {
		handleError(rw, &Oauth2Error{"invalid_request", "invalid redirect_uri"})
		return
	}
	if _, err := url.Parse(redirectURI); err != nil {
		handleError(rw, &Oauth2Error{"invalid_request", fmt.Sprintf("invalid redirect_uri: %v", err)})
		return
	}
	state := h.sessions.SaveWithState("", &Session{
		Flow:                FlowCode,
		ClientID:            clientID,
		ClientState:         values.Get("state"),
		RedirectURI:         redirectURI,
		CodeChallenge:       values.Get("code_challenge"),
		CodeChallengeMethod: values.Get("code_challenge_method"),
	})
	oauth2Config := h.cloneOauth2Config()
	http.Redirect(rw, r, oauth2Config.AuthCodeURL(state), http.StatusFound)
}
