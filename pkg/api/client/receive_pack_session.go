// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiclient

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/wrgl/core/pkg/api/payload"
	apiutils "github.com/wrgl/core/pkg/api/utils"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

type stateFn func() (stateFn, error)

type ReceivePackSession struct {
	sender  *apiutils.ObjectSender
	c       *Client
	updates map[string]*payload.Update
	state   stateFn
}

func NewReceivePackSession(db objects.Store, rs ref.Store, c *Client, updates map[string]*payload.Update, remoteRefs map[string][]byte, maxPackfileSize uint64) (*ReceivePackSession, error) {
	wants := [][]byte{}
	for _, u := range updates {
		if u.Sum != nil {
			wants = append(wants, (*u.Sum)[:])
		}
	}
	haves := make([][]byte, 0, len(remoteRefs))
	for _, sum := range remoteRefs {
		haves = append(haves, sum)
	}
	finder := apiutils.NewClosedSetsFinder(db, rs)
	_, err := finder.Process(wants, haves, true)
	if err != nil {
		return nil, err
	}
	sender, err := apiutils.NewObjectSender(db, finder.CommitsToSend(), finder.CommonCommmits(), maxPackfileSize)
	if err != nil {
		return nil, err
	}
	s := &ReceivePackSession{
		sender:  sender,
		updates: updates,
		c:       c,
	}
	s.state = s.initialize
	return s, nil
}

func (s *ReceivePackSession) initialize() (stateFn, error) {
	var nextState stateFn
	for _, u := range s.updates {
		if u.Sum != nil {
			nextState = s.sendObjects
		}
	}
	resp, err := s.c.PostUpdatesToReceivePack(s.updates)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, s.c.ErrHTTP(resp)
	}
	if ct := resp.Header.Get("Content-Type"); ct == CTJSON {
		return s.readReport(resp)
	}
	if nextState != nil {
		return nextState, nil
	}
	return s.readReport(resp)
}

func (s *ReceivePackSession) readReport(resp *http.Response) (stateFn, error) {
	if ct := resp.Header.Get("Content-Type"); ct != CTJSON {
		return nil, fmt.Errorf("unexpected content type %q", ct)
	}
	r := &payload.ReceivePackResponse{}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, r)
	if err != nil {
		return nil, err
	}
	s.updates = r.Updates
	return nil, nil
}

func (s *ReceivePackSession) sendObjects() (stateFn, error) {
	reqBody := bytes.NewBuffer(nil)
	gzw := gzip.NewWriter(reqBody)
	done, err := s.sender.WriteObjects(gzw)
	if err != nil {
		return nil, err
	}
	err = gzw.Close()
	if err != nil {
		return nil, err
	}
	resp, err := s.c.Request(http.MethodPost, "/receive-pack/", reqBody, map[string]string{
		"Content-Type":     CTPackfile,
		"Content-Encoding": "gzip",
	})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, s.c.ErrHTTP(resp)
	}
	if !done {
		return s.sendObjects, nil
	}
	return s.readReport(resp)
}

func (s *ReceivePackSession) Start() (updates map[string]*payload.Update, err error) {
	for s.state != nil {
		s.state, err = s.state()
		if err != nil {
			return nil, err
		}
	}
	return s.updates, nil
}
