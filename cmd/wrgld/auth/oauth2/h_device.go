package authoauth2

import (
	_ "embed"
	"net/http"

	"github.com/google/uuid"
)

func (h *Handler) handleDevice(rw http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeDeviceHTML(rw, http.StatusOK, &deviceTmplData{})
	case http.MethodPost:
		values, err := h.parsePOSTForm(r)
		if err != nil {
			outputHTMLError(rw, err)
			return
		}
		userCode, err := uuid.Parse(values.Get("user_code"))
		if err != nil {
			writeDeviceHTML(rw, http.StatusBadRequest, &deviceTmplData{
				ErrorMessage: "Invalid User Code",
			})
			return
		}
		ses := h.sessions.Get(userCode.String())
		if ses == nil {
			writeDeviceHTML(rw, http.StatusBadRequest, &deviceTmplData{
				ErrorMessage: "User Code not found",
			})
			return
		}
		h.sessions.Save(ses.State, ses)
		http.Redirect(rw, r, h.provider.AuthCodeURL(ses.State), http.StatusFound)
		h.sessions.Pop(userCode.String())
	default:
		writeErrorHTML(rw, http.StatusMethodNotAllowed, &errorTmplData{ErrorMessage: "method not allowed"})
	}
}
