// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package packclient

import (
	"fmt"
	"io"
	"sync"

	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/versioning"
)

const defaultHavesPerRoundTrip = 32

type Object struct {
	Type    int
	Content []byte
}

type Negotiator struct {
	c                 *Client
	db                kv.DB
	fs                kv.FileStore
	ObjectChan        chan *Object
	wants             [][]byte
	q                 *versioning.CommitsQueue
	done              bool
	popCount          int
	wg                *sync.WaitGroup
	Advertised        map[string][]byte
	havesPerRoundTrip int
}

func NewNegotiator(db kv.DB, fs kv.FileStore, wg *sync.WaitGroup, c *Client, advertised [][]byte, havesPerRoundTrip int) (*Negotiator, error) {
	if havesPerRoundTrip == 0 {
		havesPerRoundTrip = defaultHavesPerRoundTrip
	}
	neg := &Negotiator{
		c:                 c,
		db:                db,
		fs:                fs,
		ObjectChan:        make(chan *Object, 100),
		wg:                wg,
		havesPerRoundTrip: havesPerRoundTrip,
	}
	for _, b := range advertised {
		if !versioning.CommitExist(db, b) {
			neg.wants = append(neg.wants, b)
		}
	}
	if len(neg.wants) == 0 {
		return nil, fmt.Errorf("nothing wanted")
	}
	return neg, nil
}

func (n *Negotiator) popHaves() (haves [][]byte, err error) {
	if n.q == nil {
		m, err := versioning.ListAllRefs(n.db)
		if err != nil {
			return nil, err
		}
		sl := [][]byte{}
		for _, v := range m {
			sl = append(sl, v)
		}
		n.q, err = versioning.NewCommitsQueue(n.db, sl)
		if err != nil {
			return nil, err
		}
	}
	for i := 0; i < n.havesPerRoundTrip; i++ {
		sum, _, err := n.q.PopInsertParents()
		if err == io.EOF {
			n.done = true
			break
		}
		if err != nil {
			return nil, err
		}
		haves = append(haves, sum)
		n.popCount++
	}
	if n.popCount >= 256 {
		n.done = true
	}
	return
}

func (n *Negotiator) emitObjects(pr *encoding.PackfileReader) error {
	for {
		t, b, err := pr.ReadObject()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		n.wg.Add(1)
		n.ObjectChan <- &Object{t, b}
	}
	close(n.ObjectChan)
	return nil
}

func (n *Negotiator) Start() error {
	for {
		haves, err := n.popHaves()
		if err != nil {
			return err
		}
		acks, pr, err := n.c.PostUploadPack(n.wants, haves, n.done)
		if err != nil {
			return err
		}
		if acks == nil {
			defer pr.Close()
			err = n.emitObjects(pr)
			if err != nil {
				return err
			}
			break
		}
		err = n.q.RemoveAncestors(acks)
		if err != nil {
			return err
		}
		n.wants = nil
	}
	return nil
}
