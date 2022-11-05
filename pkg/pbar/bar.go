package pbar

import (
	"github.com/vbauerster/mpb/v8"
)

type Bar interface {
	Incr()
	Done()
	Abort()
	SetTotal(total int64)
	SetCurrent(cur int64)
}

type noopBar struct{}

func (b *noopBar) Incr()                {}
func (b *noopBar) Done()                {}
func (b *noopBar) Abort()               {}
func (b *noopBar) SetTotal(total int64) {}
func (b *noopBar) SetCurrent(cur int64) {}

func NewNoopBar() Bar {
	return &noopBar{}
}

type bar struct {
	b     *mpb.Bar
	p     *mpb.Progress
	total int64
}

func (b *bar) Incr() {
	b.b.IncrBy(1)
	if b.total == 0 {
		b.b.SetTotal(-1, false)
	}
}

func (b *bar) Done() {
	if b.b.IsRunning() {
		b.b.SetTotal(-1, true)
		b.b.Wait()
	}
	if b.p != nil {
		b.p.Wait()
	}
}

func (b *bar) Abort() {
	if b.b.IsRunning() {
		b.b.Abort(true)
		b.b.Wait()
	}
	if b.p != nil {
		b.p.Wait()
	}
}

func (b *bar) SetTotal(total int64) {
	b.b.SetTotal(total, false)
}
func (b *bar) SetCurrent(cur int64) {
	b.b.SetCurrent(cur)
}
