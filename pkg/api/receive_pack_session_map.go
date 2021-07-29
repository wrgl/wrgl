// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api

import "github.com/google/uuid"

type ReceivePackSessionMap struct {
	m map[string]*ReceivePackSession
}

func NewReceivePackSessionMap() *ReceivePackSessionMap {
	return &ReceivePackSessionMap{
		m: map[string]*ReceivePackSession{},
	}
}

func (m *ReceivePackSessionMap) Set(sid uuid.UUID, ses *ReceivePackSession) {
	m.m[sid.String()] = ses
}

func (m *ReceivePackSessionMap) Get(sid uuid.UUID) (ses *ReceivePackSession, ok bool) {
	ses, ok = m.m[sid.String()]
	return
}

func (m *ReceivePackSessionMap) Delete(sid uuid.UUID) {
	delete(m.m, sid.String())
}
