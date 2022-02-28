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
	Start      time.Time
	// TODO: implement ExpiresIn
}

type SessionManager struct {
	stateMap map[string]*Session
	mutex    sync.Mutex
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		stateMap: map[string]*Session{},
	}
}

func (m *SessionManager) SaveWithState(state string, ses *Session) string {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if state == "" {
		state = uuid.New().String()
	}
	m.stateMap[state] = ses
	return state
}

func (m *SessionManager) PopWithState(state string) *Session {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if v, ok := m.stateMap[state]; ok {
		delete(m.stateMap, state)
		return v
	}
	return nil
}
