// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package pack

import (
	"container/list"
	"encoding/hex"
	"io"
	"strings"

	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

type Negotiator struct {
	commons map[string]struct{}
	acks    [][]byte
	wants   map[string]struct{}
	lists   []*list.List
}

func NewNegotiator() *Negotiator {
	return &Negotiator{
		commons: map[string]struct{}{},
		wants:   map[string]struct{}{},
	}
}

func (n *Negotiator) CommonCommmits() [][]byte {
	result := [][]byte{}
	for sum := range n.commons {
		result = append(result, []byte(sum))
	}
	return result
}

func (n *Negotiator) CommitsToSend() (commits []*objects.Commit) {
	for _, l := range n.lists {
		e := l.Front()
		for e != nil {
			commits = append(commits, e.Value.(*objects.Commit))
			e = e.Next()
		}
	}
	return
}

func (n *Negotiator) ensureWantsAreReachable(queue *ref.CommitsQueue, wants [][]byte) error {
	confirmed := map[string]struct{}{}
	for _, want := range wants {
		if queue.Seen(want) {
			confirmed[string(want)] = struct{}{}
			continue
		}
		_, _, err := queue.PopUntil(want)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		confirmed[string(want)] = struct{}{}
	}
	if len(confirmed) == len(wants) {
		return nil
	}
	sums := []string{}
	for _, want := range wants {
		if _, ok := confirmed[string(want)]; ok {
			continue
		}
		sums = append(sums, hex.EncodeToString(want))
	}
	return NewBadRequestError("unrecognized wants: %s", strings.Join(sums, ", "))
}

func (n *Negotiator) findCommons(db objects.Store, queue *ref.CommitsQueue, haves [][]byte) (commons [][]byte, err error) {
	ancestors := map[string]struct{}{}
	addToCommons := func(b []byte) error {
		if _, ok := ancestors[string(b)]; ok {
			return nil
		}
		commons = append(commons, b)
		q := list.New()
		q.PushBack(b)
		for q.Len() > 0 {
			e := q.Front()
			q.Remove(e)
			sum := e.Value.([]byte)
			if _, ok := ancestors[string(sum)]; ok {
				continue
			}
			ancestors[string(sum)] = struct{}{}
			commit, err := objects.GetCommit(db, sum)
			if err != nil {
				return err
			}
			for _, parent := range commit.Parents {
				q.PushBack(parent)
			}
		}
		return nil
	}
	for _, have := range haves {
		if _, ok := ancestors[string(have)]; ok {
			continue
		}
		if queue.Seen(have) {
			err = addToCommons(have)
			if err != nil {
				return nil, err
			}
			continue
		}
		_, _, err := queue.PopUntil(have)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		err = addToCommons(have)
		if err != nil {
			return nil, err
		}
	}
	return commons, nil
}

func (n *Negotiator) findClosedSetOfObjects(db objects.Store, done bool) (err error) {
	alreadySeenCommits := map[string]struct{}{}
	wants := map[string]struct{}{}
wantsLoop:
	for want := range n.wants {
		l := list.New()
		q := list.New()
		q.PushBack([]byte(want))
		sums := [][]byte{}
		for q.Len() > 0 {
			e := q.Front()
			q.Remove(e)
			sum := e.Value.([]byte)
			sums = append(sums, sum)
			if _, ok := alreadySeenCommits[string(sum)]; ok {
				continue
			}
			if _, ok := n.commons[string(sum)]; ok {
				continue
			}
			c, err := objects.GetCommit(db, sum)
			if err != nil {
				return err
			}
			l.PushFront(c)
			// reached a root commit but still haven't seen anything in common
			if len(c.Parents) == 0 {
				// if there's nothing in common then everything including the root should be sent
				if len(n.commons) == 0 || done {
					break
				}
				wants[string(want)] = struct{}{}
				continue wantsLoop
			}
			for _, p := range c.Parents {
				q.PushBack(p)
			}
		}
		// queue is exhausted mean everything is reachable from commons
		n.lists = append(n.lists, l)
		for _, sum := range sums {
			alreadySeenCommits[string(sum)] = struct{}{}
		}
	}
	n.wants = wants
	return nil
}

// HandleUploadPackRequest parse upload-pack request and returns non-nil acks if there are ACK to send to client.
// Otherwise it returns nil acks to denote that the negotiation is done and a Packfile should be sent now.
func (n *Negotiator) HandleUploadPackRequest(db objects.Store, rs ref.Store, wants, haves [][]byte, done bool) (acks [][]byte, err error) {
	if len(n.wants) == 0 {
		// first request must have non-empty want list
		if len(wants) == 0 {
			return nil, NewBadRequestError("empty wants list")
		}
	}
	err = n.ProcessWants(db, rs, wants, haves, done)
	if err != nil {
		return
	}
	if len(n.wants) > 0 && !done {
		acks = n.acks
	}
	return
}

func (n *Negotiator) ProcessWants(db objects.Store, rs ref.Store, wants, haves [][]byte, done bool) (err error) {
	m, err := ref.ListLocalRefs(rs)
	if err != nil {
		return
	}
	sl := make([][]byte, 0, len(m))
	for _, v := range m {
		sl = append(sl, v)
	}
	queue, err := ref.NewCommitsQueue(db, sl)
	if err != nil {
		return
	}
	if len(wants) > 0 {
		err = n.ensureWantsAreReachable(queue, wants)
		if err != nil {
			return
		}
		for _, b := range wants {
			n.wants[string(b)] = struct{}{}
		}
	}
	commons, err := n.findCommons(db, queue, haves)
	if err != nil {
		return
	}
	n.acks = n.acks[:0]
	for _, b := range commons {
		n.commons[string(b)] = struct{}{}
		n.acks = append(n.acks, b)
	}
	return n.findClosedSetOfObjects(db, done)
}
