package authoauth2

import (
	"net/http"

	"github.com/google/uuid"
	server "github.com/wrgl/wrgl/wrgld/pkg/server"
)

const (
	codeDuration       = 15 * 60 // 15 minutes
	deviceCodeInterval = 1       // 1 second
)

type DeviceCodeResponse struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURI string `json:"verification_uri"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

func (h *Handler) handleDeviceCode(rw http.ResponseWriter, r *http.Request) {
	values, err := h.parsePOSTForm(r)
	if err != nil {
		outputError(rw, err)
		return
	}
	clientID := values.Get("client_id")
	if !h.validClientID(clientID) {
		outputError(rw, &Oauth2Error{"invalid_client", "unknown client"})
		return
	}
	deviceCode := uuid.New()
	userCode := uuid.New()
	state := uuid.New().String()
	h.sessions.Save(userCode.String(), &Session{
		Flow:       FlowDeviceCode,
		ClientID:   clientID,
		DeviceCode: &deviceCode,
		UserCode:   &userCode,
		State:      state,
	})
	resp := &DeviceCodeResponse{
		DeviceCode:      deviceCode.String(),
		UserCode:        userCode.String(),
		VerificationURI: h.address + "/oauth2/device/",
		ExpiresIn:       codeDuration,
		Interval:        deviceCodeInterval,
	}
	server.WriteJSON(rw, resp)
}
