package authoidc

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
)

func (h *Handler) handleCallback(rw http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	state := values.Get("state")
	if state == "" {
		writeErrorHTML(rw, http.StatusBadRequest, &errorTmplData{ErrorMessage: "state is missing"})
		return
	}
	session := h.sessions.PopWithState(state)
	if session == nil {
		writeErrorHTML(rw, http.StatusBadRequest, &errorTmplData{ErrorMessage: "invalid state"})
		return
	}
	code := values.Get("code")
	switch session.Flow {
	case FlowCode:
		uri, err := url.Parse(session.RedirectURI)
		if err != nil {
			log.Printf("error parsing redirect_uri: %v", err)
			writeErrorHTML(rw, http.StatusInternalServerError, &errorTmplData{ErrorMessage: "internal server error"})
			return
		}
		query := uri.Query()
		query.Set("state", session.ClientState)
		if errStr := values.Get("error"); errStr != "" {
			query.Set("error", errStr)
			query.Set("error_description", values.Get("error_description"))
		} else {
			h.sessions.SaveWithState(code, &Session{
				Flow:                FlowCode,
				RedirectURI:         session.RedirectURI,
				ClientID:            session.ClientID,
				CodeChallenge:       session.CodeChallenge,
				CodeChallengeMethod: session.CodeChallengeMethod,
			})
			query.Set("code", code)
		}
		uri.RawQuery = query.Encode()
		http.Redirect(rw, r, uri.String(), http.StatusFound)
	case FlowDeviceCode:
		session.Code = code
		h.sessions.SaveWithState(session.DeviceCode.String(), session)
		writeDeviceLoggedInHTML(rw, &deviceLoggedInTmplData{})
	default:
		panic(fmt.Errorf("invalid session flow %q", session.Flow))
	}
}
