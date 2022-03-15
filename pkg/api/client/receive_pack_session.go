// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiclient

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/schollz/progressbar/v3"
	"github.com/wrgl/wrgl/pkg/api/payload"
	apiutils "github.com/wrgl/wrgl/pkg/api/utils"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

type stateFn func() (stateFn, error)

type ReceivePackSession struct {
	sender  *apiutils.ObjectSender
	c       *Client
	updates map[string]*payload.Update
	state   stateFn
	pbar    *progressbar.ProgressBar
	opts    []RequestOption
}

func NewReceivePackSession(db objects.Store, rs ref.Store, c *Client, updates map[string]*payload.Update, remoteRefs map[string][]byte, maxPackfileSize uint64, pbar *progressbar.ProgressBar, opts ...RequestOption) (*ReceivePackSession, error) {
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
	finder := apiutils.NewClosedSetsFinder(db, rs, 0)
	_, err := finder.Process(wants, haves, true)
	if err != nil {
		return nil, fmt.Errorf("finder.Process error: %v", err)
	}
	coms := finder.CommitsToSend()
	for _, com := range coms {
		if !objects.TableExist(db, com.Table) {
			return nil, NewShallowCommitError(com.Sum, com.Table)
		}
	}
	sender, err := apiutils.NewObjectSender(db, coms, finder.TablesToSend(), finder.CommonCommmits(), maxPackfileSize)
	if err != nil {
		return nil, err
	}
	s := &ReceivePackSession{
		sender:  sender,
		updates: updates,
		c:       c,
		opts:    opts,
		pbar:    pbar,
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
	resp, err := s.c.PostUpdatesToReceivePack(s.updates, s.opts...)
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
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if ct := resp.Header.Get("Content-Type"); ct != CTJSON {
		return nil, fmt.Errorf("unexpected content type %q, payload was %q", ct, strings.TrimSpace(string(b)))
	}
	r := &payload.ReceivePackResponse{}
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
	done, err := s.sender.WriteObjects(gzw, s.pbar)
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
	}, s.opts...)
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
