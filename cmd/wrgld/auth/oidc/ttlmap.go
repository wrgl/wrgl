package authoidc

import (
	"container/heap"
	"sync"
	"time"
)

var defaultTTLMapIdleDuration time.Duration = time.Second * 10

type ttlItem struct {
	key    string
	expire time.Time
}

type ttlHeap []ttlItem

func (sl ttlHeap) Len() int {
	return len(sl)
}

func (sl ttlHeap) Less(i, j int) bool {
	return sl[i].expire.Before(sl[j].expire)
}

func (sl ttlHeap) Swap(i, j int) {
	sl[i], sl[j] = sl[j], sl[i]
}

func (sl *ttlHeap) Push(x interface{}) {
	*sl = append(*sl, x.(ttlItem))
}

func (sl *ttlHeap) Pop() interface{} {
	old := *sl
	n := len(old)
	x := old[n-1]
	*sl = old[0 : n-1]
	return x
}

type TTLMap struct {
	items        map[string]interface{}
	keys         *ttlHeap
	mutex        sync.Mutex
	done         chan bool
	idleDuration time.Duration
}

func NewTTLMap(idleDuration time.Duration) *TTLMap {
	if idleDuration == 0 {
		idleDuration = defaultTTLMapIdleDuration
	}
	return &TTLMap{
		items:        map[string]interface{}{},
		keys:         &ttlHeap{},
		done:         make(chan bool),
		idleDuration: idleDuration,
	}
}

func (m *TTLMap) Add(key string, obj interface{}, ttl time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.items[key] = obj
	heap.Push(m.keys, ttlItem{key, time.Now().Add(ttl)})
}

func (m *TTLMap) Pop(key string) interface{} {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if v, ok := m.items[key]; ok {
		delete(m.items, key)
		return v
	}
	return nil
}

func (m *TTLMap) removeExpiredItems() (sleepDuration time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	now := time.Now()
	for {
		n := m.keys.Len()
		if n == 0 {
			return m.idleDuration
		}
		k := heap.Pop(m.keys).(ttlItem)
		if k.expire.After(now) {
			if _, ok := m.items[k.key]; ok {
				heap.Push(m.keys, k)
				return k.expire.Sub(now)
			}
		}
		delete(m.items, k.key)
	}
}

func (m *TTLMap) StartCleanUpRoutine() {
	go func() {
		for {
			select {
			case <-m.done:
				return
			default:
				time.Sleep(m.removeExpiredItems())
			}
		}
	}()
}

func (m *TTLMap) Stop() {
	m.done <- true
}
