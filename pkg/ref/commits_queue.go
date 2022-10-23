// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ref

import (
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/wrgl/wrgl/pkg/objects"
)

const maxQueueGrow = 1 << 10

// CommitsQueue is a queue sorted by commit time (newer commit first, older commit last).
type CommitsQueue struct {
	db      objects.Store
	commits []*objects.Commit
	sums    [][]byte
	seen    map[string]struct{}
	grow    int
	mutex   sync.Mutex
}

func (q *CommitsQueue) Reset(initialSums [][]byte) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	n := len(initialSums)
	if q.commits == nil {
		q.commits = make([]*objects.Commit, 0, n)
	} else {
		q.commits = q.commits[:0]
	}
	if q.sums == nil {
		q.sums = make([][]byte, 0, n)
	} else {
		q.sums = q.sums[:0]
	}
	q.seen = map[string]struct{}{}
	for _, v := range initialSums {
		if _, ok := q.seen[string(v)]; ok {
			continue
		}
		commit, err := objects.GetCommit(q.db, v)
		if err != nil {
			return fmt.Errorf("GetCommit %x error: %v", v, err)
		}
		q.sums = append(q.sums, v)
		q.commits = append(q.commits, commit)
		q.seen[string(v)] = struct{}{}
	}
	sort.Sort(q)
	return nil
}

func NewCommitsQueue(db objects.Store, initialSums [][]byte) (*CommitsQueue, error) {
	q := &CommitsQueue{
		db:   db,
		grow: 2,
	}
	if err := q.Reset(initialSums); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *CommitsQueue) Len() int {
	return len(q.sums)
}

func (q *CommitsQueue) Less(i, j int) bool {
	return q.commits[i].Time.After(q.commits[j].Time)
}

func (q *CommitsQueue) Swap(i, j int) {
	q.commits[i], q.commits[j] = q.commits[j], q.commits[i]
	q.sums[i], q.sums[j] = q.sums[j], q.sums[i]
}

func (q *CommitsQueue) growSize() int {
	q.grow = q.grow << 1
	if q.grow > maxQueueGrow {
		q.grow = maxQueueGrow
	}
	return q.grow
}

func (q *CommitsQueue) Insert(sum []byte) (err error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.Seen(sum) {
		return
	}
	commit, err := objects.GetCommit(q.db, sum)
	if err != nil {
		return
	}
	i := sort.Search(q.Len(), func(i int) bool {
		return q.commits[i].Time.Before(commit.Time) || q.commits[i].Time.Equal(commit.Time)
	})
	n := q.Len()
	if n+1 > cap(q.sums) {
		grow := q.growSize()
		sums := make([][]byte, n+1, n+grow)
		copy(sums, q.sums)
		q.sums = sums
		commits := make([]*objects.Commit, n+1, n+grow)
		copy(commits, q.commits)
		q.commits = commits
	} else {
		q.sums = q.sums[:n+1]
		q.commits = q.commits[:n+1]
	}
	copy(q.sums[i+1:], q.sums[i:])
	copy(q.commits[i+1:], q.commits[i:])
	q.sums[i] = sum
	q.commits[i] = commit
	q.seen[string(q.sums[i])] = struct{}{}
	return nil
}

func (q *CommitsQueue) Pop() (sum []byte, commit *objects.Commit, err error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.Len() == 0 {
		return nil, nil, io.EOF
	}
	sum = q.sums[0]
	commit = q.commits[0]
	q.sums = q.sums[1:]
	q.commits = q.commits[1:]
	return
}

func (q *CommitsQueue) InsertParents(c *objects.Commit) (err error) {
	for _, p := range c.Parents {
		err = q.Insert(p)
		if err != nil {
			return
		}
	}
	return
}

func (q *CommitsQueue) PopInsertParents() (sum []byte, commit *objects.Commit, err error) {
	sum, commit, err = q.Pop()
	if err != nil {
		return
	}
	err = q.InsertParents(commit)
	return
}

func (q *CommitsQueue) PopUntil(b []byte) (sum []byte, commit *objects.Commit, err error) {
	for {
		sum, commit, err = q.PopInsertParents()
		if err == io.EOF {
			return
		}
		if err != nil {
			return
		}
		if string(sum) == string(b) {
			return
		}
	}
}

func (q *CommitsQueue) RemoveAncestors(sums [][]byte) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q2, err := NewCommitsQueue(q.db, sums)
	if err != nil {
		return err
	}
	indicesToRemove := map[int]struct{}{}
	for i, sum := range q.sums {
		if q2.Seen(sum) {
			indicesToRemove[i] = struct{}{}
			continue
		}
		for {
			ancestor, _, err := q2.PopInsertParents()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if string(sum) == string(ancestor) {
				indicesToRemove[i] = struct{}{}
				break
			}
		}
	}
	k := len(indicesToRemove)
	if k == 0 {
		return nil
	}
	n := q.Len()
	j := 0
	for i := 0; i < n; i++ {
		if _, ok := indicesToRemove[i]; ok {
			continue
		}
		q.sums[j] = q.sums[i]
		q.commits[j] = q.commits[i]
		j++
	}
	q.sums = q.sums[:n-k]
	q.commits = q.commits[:n-k]
	return nil
}

func (q *CommitsQueue) Seen(b []byte) bool {
	_, ok := q.seen[string(b)]
	return ok
}

// IsAncestorOf returns true if "commit1" is ancestor of "commit2"
func IsAncestorOf(db objects.Store, commit1, commit2 []byte) (ok bool, err error) {
	q, err := NewCommitsQueue(db, [][]byte{commit2})
	if err != nil {
		return
	}
	for {
		sum, _, err := q.PopInsertParents()
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if string(sum) == string(commit1) {
			return true, nil
		}
	}
}
