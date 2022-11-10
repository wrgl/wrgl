// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiclient

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/wrgl/wrgl/pkg/api/payload"
	apiutils "github.com/wrgl/wrgl/pkg/api/utils"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/pbar"
	"github.com/wrgl/wrgl/pkg/ref"
)

type stateFn func() (stateFn, error)

type ReceivePackSession struct {
	sender          *apiutils.ObjectSender
	c               *Client
	updates         map[string]*payload.Update
	state           stateFn
	opts            []RequestOption
	finder          *apiutils.ClosedSetsFinder
	candidateTables *list.List
	tablesToSend    map[string]struct{}
	db              objects.Store
	maxPackfileSize uint64
	barContainer    pbar.Container
	objectsBar      pbar.Bar
	buf             *bytes.Buffer
}

func NewReceivePackSession(db objects.Store, rs ref.Store, c *Client, updates map[string]*payload.Update, remoteRefs map[string][]byte, maxPackfileSize uint64, opts ...RequestOption) (*ReceivePackSession, error) {
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
	coms, err := finder.CommitsToSend()
	if err != nil {
		return nil, err
	}
	if err := NewShallowCommitError(db, rs, coms); err != nil {
		return nil, err
	}
	s := &ReceivePackSession{
		finder:          finder,
		updates:         updates,
		c:               c,
		opts:            opts,
		candidateTables: list.New(),
		buf:             bytes.NewBuffer(nil),
		db:              db,
		maxPackfileSize: maxPackfileSize,
	}
	s.tablesToSend, err = finder.TablesToSend()
	if err != nil {
		return nil, err
	}
	for sum := range s.tablesToSend {
		s.candidateTables.PushFront([]byte(sum))
	}
	s.state = s.greet
	return s, nil
}

func parseReceivePackResponse(resp *http.Response) (rpr *payload.ReceivePackResponse, err error) {
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if ct := resp.Header.Get("Content-Type"); ct != CTJSON {
		return nil, fmt.Errorf("unexpected content type %q, payload was %q", ct, strings.TrimSpace(string(b)))
	}
	rpr = &payload.ReceivePackResponse{}
	if err = json.Unmarshal(b, rpr); err != nil {
		return nil, err
	}
	return rpr, nil
}

func (s *ReceivePackSession) greet() (stateFn, error) {
	return s.negotiate(s.updates)
}

func (s *ReceivePackSession) negotiate(updates map[string]*payload.Update) (stateFn, error) {
	tbls := [][]byte{}
	for i := 0; i < 256; i++ {
		if s.candidateTables.Len() == 0 {
			break
		}
		tbls = append(tbls, s.candidateTables.Remove(s.candidateTables.Front()).([]byte))
	}
	resp, err := s.c.PostReceivePack(updates, tbls, s.opts...)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, s.c.ErrHTTP(resp)
	}
	rpr, err := parseReceivePackResponse(resp)
	if err != nil {
		return nil, err
	}
	s.c.LogResponse(resp, rpr)
	if len(rpr.Updates) > 0 {
		s.updates = rpr.Updates
		return nil, nil
	} else {
		for _, sum := range rpr.TableACKs {
			delete(s.tablesToSend, string((*sum)[:]))
		}
		if s.candidateTables.Len() == 0 {
			commits, err := s.finder.CommitsToSend()
			if err != nil {
				return nil, err
			}
			s.sender, err = apiutils.NewObjectSender(s.db, commits, s.tablesToSend, s.finder.CommonCommmits(), s.maxPackfileSize)
			if err != nil {
				return nil, err
			}
			return s.sendObjects()
		}
		return s.negotiate(nil)
	}
}

func (s *ReceivePackSession) readReport(resp *http.Response) (stateFn, error) {
	r, err := parseReceivePackResponse(resp)
	if err != nil {
		return nil, err
	}
	s.c.LogResponse(resp, r)
	s.updates = r.Updates
	return nil, nil
}

func (s *ReceivePackSession) sendObjects() (stateFn, error) {
	s.buf.Reset()
	gzw := gzip.NewWriter(s.buf)
	done, info, err := s.sender.WriteObjects(gzw, s.objectsBar)
	if err != nil {
		return nil, err
	}
	err = gzw.Close()
	if err != nil {
		return nil, err
	}
	bar := s.barContainer.NewBar(int64(s.buf.Len()), "Sending packfile", pbar.UnitKiB)
	defer bar.Abort()
	body := bar.ProxyReader(s.buf)
	defer body.Close()
	resp, err := s.c.Request(http.MethodPost, "/receive-pack/", body, map[string]string{
		"Content-Type":     CTPackfile,
		"Content-Encoding": "gzip",
	}, s.opts...)
	if err != nil {
		return nil, err
	}
	s.c.LogRequest(resp.Request, info)
	if resp.StatusCode != http.StatusOK {
		return nil, s.c.ErrHTTP(resp)
	}
	if !done {
		return s.sendObjects, nil
	}
	return s.readReport(resp)
}

func (s *ReceivePackSession) Start(pc pbar.Container) (updates map[string]*payload.Update, err error) {
	s.barContainer = pc
	s.objectsBar = s.barContainer.NewBar(-1, "Pushing objects", 0)
	defer s.objectsBar.Done()
	for s.state != nil {
		s.state, err = s.state()
		if err != nil {
			return nil, err
		}
	}
	return s.updates, nil
}
