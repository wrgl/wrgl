package pbar

import (
	"io"

	"github.com/vbauerster/mpb/v8"
)

type Bar interface {
	Incr()
	IncrBy(n int)
	ProxyReader(r io.Reader) io.ReadCloser
	Done()
	Abort()
	SetTotal(total int64)
	SetCurrent(cur int64)
}

type noopBar struct{}

func (b *noopBar) Incr()                {}
func (b *noopBar) IncrBy(n int)         {}
func (b *noopBar) Done()                {}
func (b *noopBar) Abort()               {}
func (b *noopBar) SetTotal(total int64) {}
func (b *noopBar) SetCurrent(cur int64) {}
func (b *noopBar) ProxyReader(r io.Reader) io.ReadCloser {
	return io.NopCloser(r)
}

func NewNoopBar() Bar {
	return &noopBar{}
}

type bar struct {
	b     *mpb.Bar
	c     *Container
	total int64
	name  string
	unit  int
}

func newBar(c *Container, total int64, name string, unit int) *bar {
	return &bar{
		c:     c,
		total: total,
		name:  name,
		unit:  unit,
	}
}

func (b *bar) ensureInternalBar() {
	if b.b != nil {
		return
	}
	b.c.ensureProgress()
	b.b = b.c.addBar(b.total, b.name, b.unit)
}

func (b *bar) Incr() {
	b.ensureInternalBar()
	b.b.IncrBy(1)
	if b.total == 0 {
		b.b.SetTotal(-1, false)
	}
}

func (b *bar) IncrBy(n int) {
	b.ensureInternalBar()
	b.b.IncrBy(n)
	if b.total == 0 {
		b.b.SetTotal(-1, false)
	}
}

func (b *bar) Done() {
	if b.b == nil {
		return
	}
	if b.b.IsRunning() {
		b.b.SetTotal(-1, true)
		b.b.Wait()
	}
}

func (b *bar) Abort() {
	if b.b == nil {
		return
	}
	if b.b.IsRunning() {
		b.b.Abort(true)
		b.b.Wait()
	}
}

func (b *bar) SetTotal(total int64) {
	b.ensureInternalBar()
	b.b.SetTotal(total, false)
}
func (b *bar) SetCurrent(cur int64) {
	b.ensureInternalBar()
	b.b.SetCurrent(cur)
}

func (b *bar) ProxyReader(r io.Reader) io.ReadCloser {
	b.ensureInternalBar()
	return b.b.ProxyReader(r)
}
