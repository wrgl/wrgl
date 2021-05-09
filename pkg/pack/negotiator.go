package pack

import (
	"container/list"
	"encoding/hex"
	"io"
	"strings"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/versioning"
)

type Negotiator struct {
	commons        map[string]struct{}
	wants          map[string]struct{}
	reachableWants map[string]struct{}
}

func NewNegotiator() *Negotiator {
	return &Negotiator{
		commons:        map[string]struct{}{},
		wants:          map[string]struct{}{},
		reachableWants: map[string]struct{}{},
	}
}

func mapToBytes(m map[string]struct{}) [][]byte {
	sl := make([][]byte, 0, len(m))
	for k := range m {
		sl = append(sl, []byte(k))
	}
	return sl
}

func (n *Negotiator) CommonCommits() [][]byte {
	return mapToBytes(n.commons)
}

func (n *Negotiator) ReachableWants() [][]byte {
	return mapToBytes(n.reachableWants)
}

func (n *Negotiator) ensureWantsAreReachable(queue *versioning.CommitsQueue, wants [][]byte) error {
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

func (n *Negotiator) findCommons(db kv.DB, queue *versioning.CommitsQueue, haves [][]byte) (commons [][]byte, err error) {
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
			commit, err := versioning.GetCommit(db, sum)
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

func (n *Negotiator) findClosedSetOfObjects(db kv.DB) (err error) {
	extraCommons := map[string]struct{}{}
	wants := map[string]struct{}{}
wantsLoop:
	for want := range n.wants {
		q := list.New()
		q.PushBack([]byte(want))
		sums := [][]byte{}
		for q.Len() > 0 {
			e := q.Front()
			q.Remove(e)
			sum := e.Value.([]byte)
			sums = append(sums, sum)
			if _, ok := extraCommons[string(sum)]; ok {
				continue
			}
			if _, ok := n.commons[string(sum)]; ok {
				continue
			}
			c, err := versioning.GetCommit(db, sum)
			if err != nil {
				return err
			}
			// reached a root commit but still haven't seen anything in common
			if len(c.Parents) == 0 {
				wants[string(want)] = struct{}{}
				continue wantsLoop
			}
			for _, p := range c.Parents {
				q.PushBack(p)
			}
		}
		// queue is exhausted mean everything is reachable from commons
		n.reachableWants[string(want)] = struct{}{}
		for _, sum := range sums {
			extraCommons[string(sum)] = struct{}{}
		}
	}
	n.wants = wants
	return nil
}

// HandleUploadPackRequest parse upload-pack request and returns non-nil acks if there are ACK to send to client.
// Otherwise it returns nil acks to denote that the negotiation is done and a Packfile should be sent now.
func (n *Negotiator) HandleUploadPackRequest(db kv.DB, wants, haves [][]byte, done bool) (acks [][]byte, err error) {
	if len(n.wants) == 0 {
		// first request must have non-empty want list
		if len(wants) == 0 {
			return nil, NewBadRequestError("empty wants list")
		}
	}
	m, err := versioning.ListLocalRefs(db)
	if err != nil {
		return
	}
	sl := make([][]byte, 0, len(m))
	for _, v := range m {
		sl = append(sl, v)
	}
	queue, err := versioning.NewCommitsQueue(db, sl)
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
	for _, b := range commons {
		n.commons[string(b)] = struct{}{}
	}
	err = n.findClosedSetOfObjects(db)
	if err != nil {
		return
	}
	if len(n.wants) > 0 && !done {
		acks = make([][]byte, 0, len(n.commons))
		for b := range n.commons {
			acks = append(acks, []byte(b))
		}
	}
	return
}
