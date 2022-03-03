package authoauth2

import (
	_ "embed"
	"net/http"

	"github.com/google/uuid"
)

func (h *Handler) handleDevice(rw http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeDeviceHTML(rw, &deviceTmplData{})
	case http.MethodPost:
		values, err := h.parseForm(r)
		if err != nil {
			handleError(rw, err)
			return
		}
		userCode, err := uuid.Parse(values.Get("user_code"))
		if err != nil {
			writeDeviceHTML(rw, &deviceTmplData{
				ErrorMessage: "Invalid User Code",
			})
			return
		}
		ses := h.sessions.PopWithState(userCode.String())
		if ses == nil {
			writeDeviceHTML(rw, &deviceTmplData{
				ErrorMessage: "User Code not found",
			})
			return
		}
		h.sessions.SaveWithState(ses.State, ses)
		oauth2Config := h.cloneOauth2Config()
		http.Redirect(rw, r, oauth2Config.AuthCodeURL(ses.State), http.StatusFound)
	default:
		handleError(rw, &HTTPError{http.StatusMethodNotAllowed, "method not allowed"})
	}
}
