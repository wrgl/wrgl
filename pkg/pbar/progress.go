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
	quiet bool
}

func NewContainer(out io.Writer, quiet bool) *Container {
	p := &Container{
		p:     mpb.New(mpb.WithOutput(out)),
		quiet: quiet,
	}
	return p
}

func (p *Container) NewBar(total int64, name string, unit int) Bar {
	if p.quiet {
		return &noopBar{}
	}
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
	var b = p.p.New(total,
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]"),
		options...,
	)
	b.EnableTriggerComplete()
	return &bar{
		b:     b,
		total: total,
	}
}

func (p *Container) Wait() {
	p.p.Wait()
}

func (p *Container) OverideQuiet(quiet bool) (restore func()) {
	orig := p.quiet
	p.quiet = quiet
	return func() {
		p.quiet = orig
	}
}
