// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package ref

import (
	"io"
	"sort"

	"github.com/wrgl/core/pkg/objects"
)

const maxQueueGrow = 1 << 10

type CommitsQueue struct {
	db      objects.Store
	commits []*objects.Commit
	sums    [][]byte
	seen    map[string]struct{}
	grow    int
}

func NewCommitsQueue(db objects.Store, initialSums [][]byte) (*CommitsQueue, error) {
	sums := [][]byte{}
	commits := []*objects.Commit{}
	seen := map[string]struct{}{}
	for _, v := range initialSums {
		if _, ok := seen[string(v)]; ok {
			continue
		}
		commit, err := objects.GetCommit(db, v)
		if err != nil {
			return nil, err
		}
		sums = append(sums, v)
		commits = append(commits, commit)
		seen[string(v)] = struct{}{}
	}
	q := &CommitsQueue{
		db:      db,
		sums:    sums,
		commits: commits,
		seen:    seen,
		grow:    2,
	}
	sort.Sort(q)
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
