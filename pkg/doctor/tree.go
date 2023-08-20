package doctor

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

// Tree traverses the entire commit tree to edit and modify individual commits.
// Traversal happens in 2 phases:
// 1. Go up from the head commit to the oldest ancestor commit. During this phase,
// each commit could be diagnosed and a resolution determined.
// 2. Go down from te oldest ancestor back up to the head. During this phase, each
// commit is modified, and their parent refs updated.
type Tree struct {
	db             objects.Store
	nodes          map[string]int
	count          int
	queue          *ref.CommitsQueue
	stack          []*objects.Commit
	cur            int
	mutex          sync.Mutex
	buf            *bytes.Buffer
	updatedCommits map[string][]byte
	removedCommits map[string]struct{}
}

func NewTree(db objects.Store) (t *Tree) {
	t = &Tree{
		db:             db,
		nodes:          map[string]int{},
		cur:            -1,
		buf:            bytes.NewBuffer(nil),
		updatedCommits: map[string][]byte{},
		removedCommits: map[string]struct{}{},
	}
	return t
}

// Reset repurposes Tree states to traverse a new commit tree
func (t *Tree) Reset(headSum []byte) (err error) {
	if t.queue == nil {
		t.queue, err = ref.NewCommitsQueue(t.db, [][]byte{headSum})
	} else {
		err = t.queue.Reset([][]byte{headSum})
	}
	if err != nil {
		return err
	}
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.nodes = map[string]int{}
	t.count = 0
	t.stack = t.stack[:0]
	t.cur = -1
	t.updatedCommits = map[string][]byte{}
	t.removedCommits = map[string]struct{}{}
	return nil
}

// Head gets the bottom (head) commit. It returns io.EOF if there are no commit yet
func (t *Tree) Head() (*objects.Commit, error) {
	if len(t.stack) > 0 {
		return t.stack[0], nil
	}
	return nil, io.EOF
}

// Up returns a parent commit if possible. It returns io.EOF when there are no more ancestor
func (t *Tree) Up() (*objects.Commit, error) {
	sum, com, err := t.queue.PopInsertParents()
	if err != nil && err != io.EOF {
		return com, err
	}
	if com != nil {
		t.mutex.Lock()
		defer t.mutex.Unlock()
		t.nodes[string(sum)] = t.count
		t.count += 1
		t.stack = append(t.stack, com)
		t.cur += 1
	}
	return com, err
}

func (t *Tree) updateParents(com *objects.Commit) bool {
	parents := [][]byte{}
	updated := false
	for _, sum := range com.Parents {
		if _, ok := t.removedCommits[string(sum)]; ok {
			updated = true
			continue
		}
		if v, ok := t.updatedCommits[string(sum)]; ok {
			parents = append(parents, v)
			updated = true
		} else {
			parents = append(parents, sum)
		}
	}
	com.Parents = parents
	return updated
}

func (t *Tree) Down() (*objects.Commit, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.cur < 0 {
		return nil, io.EOF
	}
	com := t.stack[t.cur]
	t.cur -= 1
	return com, nil
}

func (t *Tree) updateCommit(oldSum []byte, com *objects.Commit) ([]byte, error) {
	t.buf.Reset()
	if _, err := com.WriteTo(t.buf); err != nil {
		return nil, err
	}
	sum, err := objects.SaveCommit(t.db, t.buf.Bytes())
	if err != nil {
		return nil, err
	}
	t.updatedCommits[string(oldSum)] = sum
	return sum, nil
}

// EditCommit traverse from the current commit to its descendant commits.
func (t *Tree) EditCommit(commit []byte, edit func(com *objects.Commit) (remove, update bool, err error)) error {
	for {
		com, err := t.Down()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("commit %x not found in tree: %v", commit, io.ErrUnexpectedEOF)
		}
		oldSum := com.Sum
		parentsUpdated := t.updateParents(com)
		stop := bytes.Equal(commit, oldSum)
		var remove, updated bool
		if stop {
			remove, updated, err = edit(com)
			if err != nil {
				return err
			}
		}
		if remove {
			t.removedCommits[string(oldSum)] = struct{}{}
			return nil
		}
		if parentsUpdated || updated {
			if _, err = t.updateCommit(oldSum, com); err != nil {
				return err
			}
		}
		if stop {
			return nil
		}
	}
}

func (t *Tree) getUpdatedCommitSum(sum []byte) []byte {
	if v, ok := t.updatedCommits[string(sum)]; ok {
		return v
	}
	return sum
}

func (t *Tree) UpdateAllDescendants() ([]byte, error) {
	var sum []byte
	for {
		com, err := t.Down()
		if errors.Is(err, io.EOF) {
			if sum == nil {
				com, err = t.Head()
				if err != nil {
					return nil, err
				}
				sum = t.getUpdatedCommitSum(com.Sum)
			}
			return sum, nil
		}
		oldSum := com.Sum
		parentsUpdated := t.updateParents(com)
		if parentsUpdated {
			sum, err = t.updateCommit(oldSum, com)
			if err != nil {
				return nil, err
			}
		}
	}
}

// Position returns ancestors and children count of a commit
func (t *Tree) Position(commitSum []byte) (ancestors, descendants int, err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if c, ok := t.nodes[string(commitSum)]; ok {
		return t.count - c - 1, c, nil
	}
	return 0, 0, fmt.Errorf("commit not found")
}
