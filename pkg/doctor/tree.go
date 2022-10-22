package doctor

import (
	"fmt"
	"io"
	"sync"

	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

// Tree allows traversal in a commit tree
type Tree struct {
	db    objects.Store
	nodes map[string]int
	count int
	queue *ref.CommitsQueue
	stack []*objects.Commit
	cur   int
	mutex sync.Mutex
}

func NewTree(db objects.Store) (t *Tree) {
	t = &Tree{
		db:    db,
		nodes: map[string]int{},
		cur:   -1,
	}
	return t
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

// Down returns a child commit if possible. It returns io.EOF when there are no more children
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

// Position returns ancestors and children count of a commit
func (t *Tree) Position(commitSum []byte) (ancestors, descendants int, err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if c, ok := t.nodes[string(commitSum)]; ok {
		return t.count - c - 1, c, nil
	}
	return 0, 0, fmt.Errorf("commit not found")
}

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
	return nil
}
