// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiutils

import (
	"container/list"
	"io"

	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

// ClosedSetsFinder finds closed sets of commits to send.
// A set of commits is closed if all ancestors already exist
// in destination repository.
type ClosedSetsFinder struct {
	db      objects.Store
	rs      ref.Store
	commons map[string]struct{}
	acks    [][]byte
	Wants   map[string]struct{}
	lists   []*list.List
}

func NewClosedSetsFinder(db objects.Store, rs ref.Store) *ClosedSetsFinder {
	return &ClosedSetsFinder{
		db:      db,
		rs:      rs,
		commons: map[string]struct{}{},
		Wants:   map[string]struct{}{},
	}
}

func (f *ClosedSetsFinder) CommonCommmits() [][]byte {
	result := [][]byte{}
	for sum := range f.commons {
		result = append(result, []byte(sum))
	}
	return result
}

func (f *ClosedSetsFinder) CommitsToSend() (commits []*objects.Commit) {
	for _, l := range f.lists {
		e := l.Front()
		for e != nil {
			commits = append(commits, e.Value.(*objects.Commit))
			e = e.Next()
		}
	}
	return
}

func (f *ClosedSetsFinder) ensureWantsAreReachable(queue *ref.CommitsQueue, wants [][]byte) error {
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
	sums := [][]byte{}
	for _, want := range wants {
		if _, ok := confirmed[string(want)]; ok {
			continue
		}
		sums = append(sums, want)
	}
	return &UnrecognizedWantsError{sums: sums}
}

func (f *ClosedSetsFinder) findCommons(queue *ref.CommitsQueue, haves [][]byte) (commons [][]byte, err error) {
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
			commit, err := objects.GetCommit(f.db, sum)
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

func (f *ClosedSetsFinder) findClosedSetOfObjects(done bool) (err error) {
	alreadySeenCommits := map[string]struct{}{}
	wants := map[string]struct{}{}
wantsLoop:
	for want := range f.Wants {
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
			if _, ok := f.commons[string(sum)]; ok {
				continue
			}
			c, err := objects.GetCommit(f.db, sum)
			if err != nil {
				return err
			}
			l.PushFront(c)
			// reached a root commit but still haven't seen anything in common
			if len(c.Parents) == 0 {
				// if there's nothing in common then everything including the root should be sent
				if len(f.commons) == 0 || done {
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
		f.lists = append(f.lists, l)
		for _, sum := range sums {
			alreadySeenCommits[string(sum)] = struct{}{}
		}
	}
	f.Wants = wants
	return nil
}

func (f *ClosedSetsFinder) Process(wants, haves [][]byte, done bool) (acks [][]byte, err error) {
	m, err := ref.ListLocalRefs(f.rs)
	if err != nil {
		return
	}
	sl := make([][]byte, 0, len(m))
	for _, v := range m {
		sl = append(sl, v)
	}
	queue, err := ref.NewCommitsQueue(f.db, sl)
	if err != nil {
		return
	}
	if len(wants) > 0 {
		err = f.ensureWantsAreReachable(queue, wants)
		if err != nil {
			return
		}
		for _, b := range wants {
			f.Wants[string(b)] = struct{}{}
		}
	}
	commons, err := f.findCommons(queue, haves)
	if err != nil {
		return
	}
	f.acks = f.acks[:0]
	for _, b := range commons {
		f.commons[string(b)] = struct{}{}
		f.acks = append(f.acks, b)
	}
	err = f.findClosedSetOfObjects(done)
	if err != nil {
		return
	}
	return f.acks, nil
}
