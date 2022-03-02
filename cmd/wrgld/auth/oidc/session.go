package authoidc

import (
	"sync"
	"time"

	"github.com/google/uuid"
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
	stateMap *TTLMap
	mutex    sync.Mutex
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		stateMap: NewTTLMap(0),
	}
}

func (m *SessionManager) SaveWithState(state string, ses *Session) string {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if state == "" {
		state = uuid.New().String()
	}
	m.stateMap.Add(state, ses, codeDuration*time.Second)
	return state
}

func (m *SessionManager) PopWithState(state string) *Session {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if v := m.stateMap.Pop(state); v != nil {
		return v.(*Session)
	}
	return nil
}
