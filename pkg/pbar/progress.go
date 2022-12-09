package pbar

import (
	"io"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

const (
	UnitKiB int = decor.UnitKiB
	UnitKB  int = decor.UnitKB
)

type Container struct {
	p     *mpb.Progress
	out   io.Writer
	quiet bool
}

func NewContainer(out io.Writer, quiet bool) *Container {
	c := &Container{
		out:   out,
		quiet: quiet,
	}
	return c
}

func (c *Container) ensureProgress() {
	if c.p == nil {
		c.p = mpb.New(mpb.WithOutput(c.out))
	}
}

func (c *Container) NewBar(total int64, name string, unit int) Bar {
	if c.quiet {
		return &noopBar{}
	}
	return newBar(c, total, name, unit)
}

func (c *Container) addBar(total int64, name string, unit int) *mpb.Bar {
	pairFmt := "%d / %d"
	if unit != 0 {
		pairFmt = "% .2f / % .2f"
	}
	options := []mpb.BarOption{
		mpb.PrependDecorators(decor.Name(name, decor.WC{W: len(name) + 1, C: decor.DidentRight}), decor.Counters(unit, pairFmt)),
		mpb.BarRemoveOnComplete(),
	}
	if total > 0 {
		options = append(options,
			mpb.AppendDecorators(decor.Percentage(decor.WC{W: 5, C: decor.DidentRight}), decor.Elapsed(decor.ET_STYLE_GO)),
		)
	} else {
		options = append(options,
			mpb.AppendDecorators(decor.Elapsed(decor.ET_STYLE_GO)),
		)
	}
	b := c.p.New(total,
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]"),
		options...,
	)
	b.EnableTriggerComplete()
	return b
}

func (c *Container) Wait() {
	if c.p == nil {
		return
	}
	c.p.Wait()
	c.p = nil
}

func (c *Container) OverideQuiet(quiet bool) (restore func()) {
	orig := c.quiet
	c.quiet = quiet
	return func() {
		c.quiet = orig
	}
}
