package server

import (
	"time"

	"github.com/google/uuid"

	wrgldutils "github.com/wrgl/wrgl/wrgld/pkg/utils"
)

const defaultSessionTTL = 24 * time.Hour

type UploadPackSessionMap struct {
	m   *wrgldutils.TTLMap
	ttl time.Duration
}

func NewUploadPackSessionMap(idle, ttl time.Duration) *UploadPackSessionMap {
	if ttl == 0 {
		ttl = defaultSessionTTL
	}
	m := &UploadPackSessionMap{
		m:   wrgldutils.NewTTLMap(idle),
		ttl: ttl,
	}
	m.m.StartCleanUpRoutine()
	return m
}

func (m *UploadPackSessionMap) Set(sid uuid.UUID, ses *UploadPackSession) {
	m.m.Add(sid.String(), ses, m.ttl)
}

func (m *UploadPackSessionMap) Get(sid uuid.UUID) (ses *UploadPackSession, ok bool) {
	if v := m.m.Get(sid.String()); v != nil {
		return v.(*UploadPackSession), true
	}
	return nil, false
}

func (m *UploadPackSessionMap) Delete(sid uuid.UUID) {
	m.m.Pop(sid.String())
}

func (m *UploadPackSessionMap) Stop() {
	m.m.Stop()
}
