// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package pack

import (
	"compress/gzip"
	"encoding/hex"
	"io"
	"net/http"
	"strings"

	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/objects"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/ref"
)

const (
	uploadPackSessionCookie = "upload-pack-session-id"
	CTUploadPackResult      = "application/x-wrgl-upload-pack-result"
	CTPackfile              = "application/x-wrgl-packfile"
)

type stateFn func(rw http.ResponseWriter, r *http.Request) (nextState stateFn)

func parseUploadPackRequest(r io.Reader) (wants, haves [][]byte, done bool, err error) {
	parser := encoding.NewParser(r)
	var s string
reading:
	for {
		s, err = encoding.ReadPktLine(parser)
		if err != nil {
			return
		}
		if s == "" {
			break
		}
		fields := strings.Fields(s)
		switch fields[0] {
		case "done":
			done = true
			break reading
		case "want":
			b := make([]byte, 16)
			_, err = hex.Decode(b, []byte(fields[1]))
			if err != nil {
				return
			}
			wants = append(wants, b)
		case "have":
			b := make([]byte, 16)
			_, err = hex.Decode(b, []byte(fields[1]))
			if err != nil {
				return
			}
			haves = append(haves, b)
		default:
			err = NewBadRequestError("unrecognized command %q", fields[0])
			return
		}
	}
	return
}

type UploadPackSession struct {
	db              objects.Store
	id              string
	finder          *packutils.ClosedSetsFinder
	sender          *packutils.ObjectSender
	path            string
	state           stateFn
	maxPackfileSize uint64
}

func NewUploadPackSession(db objects.Store, rs ref.Store, path string, id string, maxPackfileSize uint64) *UploadPackSession {
	s := &UploadPackSession{
		db:              db,
		id:              id,
		path:            path,
		finder:          packutils.NewClosedSetsFinder(db, rs),
		maxPackfileSize: maxPackfileSize,
	}
	s.state = s.greet
	return s
}

func (s *UploadPackSession) sendACKs(rw http.ResponseWriter, acks [][]byte) (nextState stateFn) {
	rw.Header().Set("Content-Type", CTUploadPackResult)
	http.SetCookie(rw, &http.Cookie{
		Name:     uploadPackSessionCookie,
		Value:    s.id,
		Path:     s.path,
		HttpOnly: true,
		MaxAge:   3600 * 24,
	})
	buf := misc.NewBuffer(nil)
	for _, ack := range acks {
		err := encoding.WritePktLine(rw, buf, "ACK "+hex.EncodeToString(ack))
		if err != nil {
			panic(err)
		}
	}
	err := encoding.WritePktLine(rw, buf, "NAK")
	if err != nil {
		panic(err)
	}
	return s.negotiate
}

func (s *UploadPackSession) sendPackfile(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	var err error
	if s.sender == nil {
		s.sender, err = packutils.NewObjectSender(s.db, s.finder.CommitsToSend(), s.finder.CommonCommmits(), s.maxPackfileSize)
		if err != nil {
			panic(err)
		}
	}

	rw.Header().Set("Content-Type", CTPackfile)
	rw.Header().Set("Content-Encoding", "gzip")
	http.SetCookie(rw, &http.Cookie{
		Name:     uploadPackSessionCookie,
		Value:    s.id,
		Path:     s.path,
		HttpOnly: true,
		MaxAge:   3600 * 24,
	})

	gzw := gzip.NewWriter(rw)
	defer gzw.Close()

	done, err := s.sender.WriteObjects(gzw)
	if err != nil {
		panic(err)
	}
	if done {
		return nil
	}
	return s.sendPackfile
}

func (s *UploadPackSession) findClosedSets(rw http.ResponseWriter, r *http.Request, wants, haves [][]byte, done bool) (nextState stateFn) {
	acks, err := s.finder.Process(wants, haves, done)
	if err != nil {
		if v, ok := err.(*packutils.UnrecognizedWantsError); ok {
			http.Error(rw, v.Error(), http.StatusBadRequest)
			return nil
		}
		panic(err)
	}
	if len(acks) > 0 && len(s.finder.Wants) > 0 && !done {
		return s.sendACKs(rw, acks)
	}
	return s.sendPackfile(rw, r)
}

func (s *UploadPackSession) greet(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	wants, haves, done, err := parseUploadPackRequest(r.Body)
	if err != nil {
		panic(err)
	}
	if len(wants) == 0 {
		http.Error(rw, "empty wants list", http.StatusBadRequest)
		return nil
	}
	return s.findClosedSets(rw, r, wants, haves, done)
}

func (s *UploadPackSession) negotiate(rw http.ResponseWriter, r *http.Request) (nextState stateFn) {
	wants, haves, done, err := parseUploadPackRequest(r.Body)
	if err != nil {
		panic(err)
	}
	return s.findClosedSets(rw, r, wants, haves, done)
}

// ServeHTTP negotiates which commits to be sent and send them in one or more packfiles,
// returns true when this session is completed and should be removed.
func (s *UploadPackSession) ServeHTTP(rw http.ResponseWriter, r *http.Request) bool {
	s.state = s.state(rw, r)
	return s.state == nil
}
