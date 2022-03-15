package server

import "github.com/google/uuid"

type UploadPackSessionMap struct {
	m map[string]*UploadPackSession
}

func NewUploadPackSessionMap() *UploadPackSessionMap {
	return &UploadPackSessionMap{
		m: map[string]*UploadPackSession{},
	}
}

func (m *UploadPackSessionMap) Set(sid uuid.UUID, ses *UploadPackSession) {
	m.m[sid.String()] = ses
}

func (m *UploadPackSessionMap) Get(sid uuid.UUID) (ses *UploadPackSession, ok bool) {
	ses, ok = m.m[sid.String()]
	return
}

func (m *UploadPackSessionMap) Delete(sid uuid.UUID) {
	delete(m.m, sid.String())
}
