// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiutils

import (
	"container/list"
	"fmt"
	"io"

	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

// ClosedSetsFinder finds closed sets of commits to send.
// A set of commits is closed if all ancestors already exist
// in destination repository.
type ClosedSetsFinder struct {
	db            objects.Store
	rs            ref.Store
	commons       map[string]struct{}
	acks          [][]byte
	Wants         map[string]struct{}
	commitLists   []*list.List
	tableSumLists []*list.List
	depth         int
}

func NewClosedSetsFinder(db objects.Store, rs ref.Store, depth int) *ClosedSetsFinder {
	return &ClosedSetsFinder{
		db:      db,
		rs:      rs,
		commons: map[string]struct{}{},
		Wants:   map[string]struct{}{},
		depth:   depth,
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
	for _, l := range f.commitLists {
		e := l.Front()
		for e != nil {
			commits = append(commits, e.Value.(*objects.Commit))
			e = e.Next()
		}
	}
	return
}

func (f *ClosedSetsFinder) TablesToSend() (tableSums map[string]struct{}) {
	tableSums = map[string]struct{}{}
	for _, l := range f.tableSumLists {
		e := l.Front()
		for e != nil {
			tableSums[string(e.Value.([]byte))] = struct{}{}
			e = e.Next()
		}
	}
	return
}

func (f *ClosedSetsFinder) isFullCommit(com *objects.Commit, sum []byte) (ok bool, err error) {
	if com == nil {
		com, err = objects.GetCommit(f.db, sum)
		if err != nil {
			return
		}
	}
	return objects.TableExist(f.db, com.Table), nil
}

func (f *ClosedSetsFinder) ensureWantsAreReachable(queue *ref.CommitsQueue, wants [][]byte) error {
	confirmed := map[string]struct{}{}
	for _, want := range wants {
		if queue.Seen(want) {
			if ok, err := f.isFullCommit(nil, want); err != nil {
				return err
			} else if ok {
				confirmed[string(want)] = struct{}{}
			}
			continue
		}
		_, com, err := queue.PopUntil(want)
		if err != nil && err != io.EOF {
			return err
		} else if com != nil {
			if ok, err := f.isFullCommit(com, nil); err != nil {
				return err
			} else if ok {
				confirmed[string(want)] = struct{}{}
			}
		}
		if err == io.EOF {
			break
		}
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
		// ensure all ancestors of b ends up in ancestors map
		for q.Len() > 0 {
			sum := q.Remove(q.Front()).([]byte)
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

type commitDepth struct {
	sum   []byte
	depth int
}

func (f *ClosedSetsFinder) findClosedSetOfObjects(done bool) (err error) {
	alreadySeenCommits := map[string]struct{}{}
	wants := map[string]struct{}{}
wantsLoop:
	for want := range f.Wants {
		commitList := list.New()
		tableList := list.New()
		q := list.New()
		q.PushBack(commitDepth{[]byte(want), 0})
		sums := [][]byte{}
		for q.Len() > 0 {
			cd := q.Remove(q.Front()).(commitDepth)
			sums = append(sums, cd.sum)
			if _, ok := alreadySeenCommits[string(cd.sum)]; ok {
				continue
			}
			if _, ok := f.commons[string(cd.sum)]; ok {
				continue
			}
			c, err := objects.GetCommit(f.db, cd.sum)
			if err != nil {
				return err
			}
			commitList.PushFront(c)
			if f.depth == 0 || cd.depth < f.depth {
				tableList.PushFront(c.Table)
			}
			// reached a root commit but still haven't seen anything in common
			if len(c.Parents) == 0 && len(f.commons) > 0 && !done {
				// preserve want for the next time findClosedSetOfObjects is called
				wants[string(want)] = struct{}{}
				continue wantsLoop
			}
			for _, p := range c.Parents {
				q.PushBack(commitDepth{p, cd.depth + 1})
			}
		}
		// queue is exhausted mean everything is reachable from commons
		f.commitLists = append(f.commitLists, commitList)
		f.tableSumLists = append(f.tableSumLists, tableList)
		for _, sum := range sums {
			alreadySeenCommits[string(sum)] = struct{}{}
		}
	}
	f.Wants = wants
	return nil
}

func (f *ClosedSetsFinder) Process(wants, haves [][]byte, done bool) (acks [][]byte, err error) {
	m, err := ref.ListAllRefs(f.rs)
	if err != nil {
		return nil, fmt.Errorf("ListAllRefs error: %v", err)
	}
	sl := make([][]byte, 0, len(m))
	for _, v := range m {
		sl = append(sl, v)
	}
	queue, err := ref.NewCommitsQueue(f.db, sl)
	if err != nil {
		return nil, fmt.Errorf("NewCommitsQueue error: %v", err)
	}
	if len(wants) > 0 {
		err = f.ensureWantsAreReachable(queue, wants)
		if err != nil {
			return nil, fmt.Errorf("ensureWantsAreReachable error: %v", err)
		}
		for _, b := range wants {
			f.Wants[string(b)] = struct{}{}
		}
	}
	commons, err := f.findCommons(queue, haves)
	if err != nil {
		return nil, fmt.Errorf("findCommons error: %v", err)
	}
	f.acks = f.acks[:0]
	for _, b := range commons {
		f.commons[string(b)] = struct{}{}
		f.acks = append(f.acks, b)
	}
	err = f.findClosedSetOfObjects(done)
	if err != nil {
		return nil, fmt.Errorf("findClosedSetOfObjects error: %v", err)
	}
	return f.acks, nil
}
