// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiclient

import (
	"fmt"
	"io"

	apiutils "github.com/wrgl/wrgl/pkg/api/utils"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

const defaultHavesPerRoundTrip = 32

type UploadPackSession struct {
	c                 *Client
	db                objects.Store
	receiver          *apiutils.ObjectReceiver
	rs                ref.Store
	wants             [][]byte
	q                 *ref.CommitsQueue
	popCount          int
	havesPerRoundTrip int
	opts              []RequestOption
}

func NewUploadPackSession(db objects.Store, rs ref.Store, c *Client, advertised [][]byte, havesPerRoundTrip int, opts ...RequestOption) (*UploadPackSession, error) {
	if havesPerRoundTrip == 0 {
		havesPerRoundTrip = defaultHavesPerRoundTrip
	}
	neg := &UploadPackSession{
		c:                 c,
		db:                db,
		rs:                rs,
		havesPerRoundTrip: havesPerRoundTrip,
		opts:              opts,
	}
	for _, b := range advertised {
		if !objects.CommitExist(db, b) {
			neg.wants = append(neg.wants, b)
		}
	}
	if len(neg.wants) == 0 {
		return nil, fmt.Errorf("nothing wanted")
	}
	neg.receiver = apiutils.NewObjectReceiver(db, neg.wants, nil)
	return neg, nil
}

func (n *UploadPackSession) popHaves() (haves [][]byte, done bool, err error) {
	if n.q == nil {
		m, err := ref.ListAllRefs(n.rs)
		if err != nil {
			return nil, false, err
		}
		sl := [][]byte{}
		for _, v := range m {
			sl = append(sl, v)
		}
		n.q, err = ref.NewCommitsQueue(n.db, sl)
		if err != nil {
			return nil, false, err
		}
	}
	for i := 0; i < n.havesPerRoundTrip; i++ {
		sum, _, err := n.q.PopInsertParents()
		if err == io.EOF {
			done = true
			break
		}
		if err != nil {
			return nil, false, err
		}
		haves = append(haves, sum)
		n.popCount++
	}
	if n.popCount >= 256 {
		done = true
	}
	return
}

func (n *UploadPackSession) Start() ([][]byte, error) {
	for {
		haves, done, err := n.popHaves()
		if err != nil {
			return nil, err
		}
		acks, pr, err := n.c.PostUploadPack(n.wants, haves, done, n.opts...)
		if err != nil {
			return nil, err
		}
		n.wants = nil
		if len(acks) == 0 {
			defer pr.Close()
			doneReceiving, err := n.receiver.Receive(pr)
			if err != nil {
				return nil, err
			}
			if doneReceiving {
				break
			}
		}
		err = n.q.RemoveAncestors(acks)
		if err != nil {
			return nil, err
		}
	}
	return n.receiver.ReceivedCommits, nil
}
