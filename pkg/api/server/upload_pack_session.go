// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/google/uuid"
	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/api/payload"
	apiutils "github.com/wrgl/wrgl/pkg/api/utils"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

type stateFn func(rw http.ResponseWriter, r *http.Request) (nextState stateFn)

func parseUploadPackRequest(r *http.Request) (req *payload.UploadPackRequest, err error) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}
	req = &payload.UploadPackRequest{}
	err = json.Unmarshal(b, req)
	if err != nil {
		return nil, err
	}
	return
}

type UploadPackSession struct {
	db              objects.Store
	id              uuid.UUID
	finder          *apiutils.ClosedSetsFinder
	sender          *apiutils.ObjectSender
	state           stateFn
	rs              ref.Store
	maxPackfileSize uint64
}

func NewUploadPackSession(db objects.Store, rs ref.Store, id uuid.UUID, maxPackfileSize uint64) *UploadPackSession {
	s := &UploadPackSession{
		db:              db,
		id:              id,
		rs:              rs,
		maxPackfileSize: maxPackfileSize,
	}
	s.state = s.greet
	return s
}

func (s *UploadPackSession) sendACKs(rw http.ResponseWriter, r *http.Request, acks [][]byte) (nextState stateFn) {
	rw.Header().Set("Content-Type", api.CTJSON)
	http.SetCookie(rw, &http.Cookie{
		Name:     api.CookieUploadPackSession,
		Value:    s.id.String(),
		Path:     r.URL.Path,
		HttpOnly: true,
		MaxAge:   3600 * 3,
	})
	resp := &payload.UploadPackResponse{}
	for _, ack := range acks {
		resp.ACKs = payload.AppendHex(resp.ACKs, ack)
	}
	b, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	_, err = rw.Write(b)
	if err != nil {
		panic(err)
	}
	return s.negotiate
}

func (s *UploadPackSession) sendPackfile(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	var err error
	if s.sender == nil {
		s.sender, err = apiutils.NewObjectSender(s.db, s.finder.CommitsToSend(), s.finder.TablesToSend(), s.finder.CommonCommmits(), s.maxPackfileSize)
		if err != nil {
			panic(err)
		}
	}

	rw.Header().Set("Content-Type", api.CTPackfile)
	rw.Header().Set("Content-Encoding", "gzip")
	rw.Header().Set("Trailer", api.HeaderPurgeUploadPackSession)
	http.SetCookie(rw, &http.Cookie{
		Name:     api.CookieUploadPackSession,
		Value:    s.id.String(),
		Path:     r.URL.Path,
		HttpOnly: true,
		MaxAge:   3600 * 3,
	})

	gzw := gzip.NewWriter(rw)
	defer gzw.Close()

	done, err := s.sender.WriteObjects(gzw)
	if err != nil {
		panic(err)
	}
	if done {
		// TODO: figure out whether to enable this trailer once more servers can deal with it?
		// rw.Header().Set(api.HeaderPurgeUploadPackSession, "true")
		return nil
	}
	return s.sendPackfile
}

func (s *UploadPackSession) findClosedSets(rw http.ResponseWriter, r *http.Request, req *payload.UploadPackRequest) (nextState stateFn) {
	acks, err := s.finder.Process(payload.HexSliceToBytesSlice(req.Wants), payload.HexSliceToBytesSlice(req.Haves), req.Done)
	if err != nil {
		if v, ok := err.(*apiutils.UnrecognizedWantsError); ok {
			SendError(rw, http.StatusBadRequest, v.Error())
			return nil
		}
		panic(err)
	}
	if len(acks) > 0 && len(s.finder.Wants) > 0 && !req.Done {
		return s.sendACKs(rw, r, acks)
	}
	return s.sendPackfile(rw, r)
}

func (s *UploadPackSession) greet(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	req, err := parseUploadPackRequest(r)
	if err != nil {
		panic(err)
	}
	s.finder = apiutils.NewClosedSetsFinder(s.db, s.rs, req.Depth)
	if len(req.Wants) == 0 {
		SendError(rw, http.StatusBadRequest, "empty wants list")
		return nil
	}
	return s.findClosedSets(rw, r, req)
}

func (s *UploadPackSession) negotiate(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	req, err := parseUploadPackRequest(r)
	if err != nil {
		panic(err)
	}
	return s.findClosedSets(rw, r, req)
}

// ServeHTTP negotiates which commits to be sent and send them in one or more packfiles,
// returns true when this session is completed and should be removed.
func (s *UploadPackSession) ServeHTTP(rw http.ResponseWriter, r *http.Request) bool {
	if ct := r.Header.Get("Content-Type"); ct != api.CTJSON {
		SendError(rw, http.StatusUnsupportedMediaType, "")
		return true
	}
	s.state = s.state(rw, r)
	return s.state == nil
}
