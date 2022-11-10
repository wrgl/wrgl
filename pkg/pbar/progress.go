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

type Container interface {
	NewBar(total int64, name string, unit int) Bar
	Wait()
}

type noopContainer struct{}

func (p *noopContainer) NewBar(total int64, name string, unit int) Bar {
	return &noopBar{}
}

func (p *noopContainer) Wait() {}

func NewNoopContainer() Container {
	return &noopContainer{}
}

type container struct {
	p *mpb.Progress
}

func NewContainer(out io.Writer) Container {
	p := &container{
		p: mpb.New(mpb.WithOutput(out)),
	}
	return p
}

func (p *container) NewBar(total int64, name string, unit int) Bar {
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

func (p *container) Wait() {
	p.p.Wait()
}
