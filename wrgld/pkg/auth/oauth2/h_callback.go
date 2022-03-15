package authoauth2

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
)

func (h *Handler) handleCallback(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErrorHTML(rw, http.StatusMethodNotAllowed, &errorTmplData{ErrorMessage: "method not allowed"})
		return
	}
	values := r.URL.Query()
	state := values.Get("state")
	if state == "" {
		writeErrorHTML(rw, http.StatusBadRequest, &errorTmplData{ErrorMessage: "state is missing"})
		return
	}
	session := h.sessions.Get(state)
	if session == nil {
		writeErrorHTML(rw, http.StatusBadRequest, &errorTmplData{ErrorMessage: "invalid state"})
		return
	}
	code := values.Get("code")
	if code == "" {
		writeErrorHTML(rw, http.StatusBadRequest, &errorTmplData{ErrorMessage: "code is missing"})
		return
	}
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
			h.sessions.Save(code, &Session{
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
		h.sessions.Save(session.DeviceCode.String(), session)
		writeDeviceLoggedInHTML(rw, &deviceLoggedInTmplData{})
	default:
		panic(fmt.Errorf("invalid session flow %q", session.Flow))
	}
	h.sessions.Pop(state)
}
