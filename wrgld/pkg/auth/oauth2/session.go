package authoauth2

import (
	"time"

	"github.com/google/uuid"

	wrgldutils "github.com/wrgl/wrgl/wrgld/pkg/utils"
)

type Flow string

const (
	FlowCode       Flow = "code"
	FlowDeviceCode Flow = "device_code"
)

type Session struct {
	Flow     Flow
	ClientID string

	// code flow fields
	RedirectURI string
	ClientState string

	// PKCE
	CodeChallenge       string
	CodeChallengeMethod string

	// device code flow fields
	DeviceCode *uuid.UUID
	UserCode   *uuid.UUID
	State      string
	Code       string
}

type SessionManager struct {
	stateMap *wrgldutils.TTLMap
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		stateMap: wrgldutils.NewTTLMap(0),
	}
}

func (m *SessionManager) Save(state string, ses *Session) string {
	if state == "" {
		state = uuid.New().String()
	}
	m.stateMap.Add(state, ses, codeDuration*time.Second)
	return state
}

func (m *SessionManager) Get(state string) *Session {
	if v := m.stateMap.Get(state); v != nil {
		return v.(*Session)
	}
	return nil
}

func (m *SessionManager) Pop(state string) *Session {
	if v := m.stateMap.Pop(state); v != nil {
		return v.(*Session)
	}
	return nil
}
