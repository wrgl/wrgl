package progress

import (
	"time"
)

type Event struct {
	Progress int64
	Total    int64
}

type Tracker interface {
	Duration() time.Duration
	Current() int64
	SetCurrent(int64)
	Total() int64
	SetTotal(int64)
	Chan() <-chan Event
	Run()
	Stop()
}

type tracker struct {
	current int64
	total   int64
	ticker  *time.Ticker
	c       chan Event
	d       time.Duration
	stopped bool
}

func NewTracker(d time.Duration, total int64) Tracker {
	if d == 0 {
		// never produce any tick event
		d = (1 << 20) * time.Hour
	}
	return &tracker{
		d:     d,
		total: total,
	}
}

func (t *tracker) Current() int64 {
	return t.current
}

func (t *tracker) SetCurrent(n int64) {
	t.current = n
}

func (t *tracker) Total() int64 {
	return t.total
}

func (t *tracker) SetTotal(n int64) {
	t.total = n
}

func (t *tracker) Chan() <-chan Event {
	if t.c == nil {
		t.c = make(chan Event)
	}
	return t.c
}

func (t *tracker) Duration() time.Duration {
	return t.d
}

func (t *tracker) Stop() {
	if t.ticker != nil {
		t.ticker.Stop()
	}
	if t.c != nil {
		close(t.c)
	}
	t.stopped = true
}

func (t *tracker) Run() {
	if t.ticker == nil {
		t.ticker = time.NewTicker(t.d)
	}
	for range t.ticker.C {
		if t.stopped {
			break
		}
		t.c <- Event{
			Progress: t.current,
			Total:    t.total,
		}
	}
}

type joinedTracker struct {
	trackers []Tracker
	d        time.Duration
	c        chan Event
	ticker   *time.Ticker
	stopped  bool
}

func JoinTrackers(sl ...Tracker) Tracker {
	n := len(sl)
	var totalD time.Duration
	for _, t := range sl {
		totalD += t.Duration()
		go t.Run()
	}
	d := time.Duration(int64(totalD) / int64(n))
	return &joinedTracker{
		d:        d,
		trackers: sl,
	}
}

func (t *joinedTracker) Duration() time.Duration {
	return t.d
}

func (t *joinedTracker) Current() int64 {
	var c int64
	for _, t := range t.trackers {
		c += t.Current()
	}
	return c
}

func (t *joinedTracker) SetCurrent(int64) {}

func (t *joinedTracker) Total() int64 {
	var c int64
	for _, t := range t.trackers {
		c += t.Total()
	}
	return c
}

func (t *joinedTracker) SetTotal(int64) {}

func (t *joinedTracker) Chan() <-chan Event {
	if t.c == nil {
		t.c = make(chan Event)
	}
	return t.c
}

func (t *joinedTracker) Run() {
	if t.ticker == nil {
		t.ticker = time.NewTicker(t.d)
	}
	for range t.ticker.C {
		if t.stopped {
			break
		}
		t.c <- Event{
			Progress: t.Current(),
			Total:    t.Total(),
		}
	}
}

func (t *joinedTracker) Stop() {
	if t.ticker != nil {
		t.ticker.Stop()
	}
	if t.c != nil {
		close(t.c)
	}
	t.stopped = true
}
