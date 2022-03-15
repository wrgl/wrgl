package server

import (
	"time"

	"github.com/google/uuid"
	wrgldutils "github.com/wrgl/wrgl/wrgld/pkg/utils"
)

type ReceivePackSessionMap struct {
	m   *wrgldutils.TTLMap
	ttl time.Duration
}

func NewReceivePackSessionMap(idle, ttl time.Duration) *ReceivePackSessionMap {
	if ttl == 0 {
		ttl = defaultSessionTTL
	}
	m := &ReceivePackSessionMap{
		m:   wrgldutils.NewTTLMap(idle),
		ttl: ttl,
	}
	m.m.StartCleanUpRoutine()
	return m
}

func (m *ReceivePackSessionMap) Set(sid uuid.UUID, ses *ReceivePackSession) {
	m.m.Add(sid.String(), ses, m.ttl)
}

func (m *ReceivePackSessionMap) Get(sid uuid.UUID) (ses *ReceivePackSession, ok bool) {
	if v := m.m.Get(sid.String()); v != nil {
		return v.(*ReceivePackSession), true
	}
	return nil, false
}

func (m *ReceivePackSessionMap) Delete(sid uuid.UUID) {
	m.m.Pop(sid.String())
}

func (m *ReceivePackSessionMap) Stop() {
	m.m.Stop()
}
