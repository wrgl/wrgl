package widgets

import (
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func styleArray(baseStyle tcell.Style, width int, styleRanges map[tcell.Style][2]int) []tcell.Style {
	sl := make([]tcell.Style, width)
	for i := 0; i < width; i++ {
		sl[i] = baseStyle
	}
	for st, ints := range styleRanges {
		for i := ints[0]; i < ints[1]; i++ {
			sl[i] = st
		}
	}
	return sl
}

func assertDrew(t *testing.T, screen tcell.SimulationScreen, row int, text string, styles []tcell.Style) {
	t.Helper()
	screen.Show()
	cells, width, height := screen.GetContents()
	require.GreaterOrEqual(t, height, row)
	b := []byte{}
	st := []tcell.Style{}
	for i := 0; i < width; i++ {
		b = append(b, cells[row*width+i].Bytes...)
		st = append(st, cells[row*width+i].Style)
	}
	assert.Equal(t, text, string(b))
	assert.Equal(t, styles, st)
}

func TestProgressBar(t *testing.T) {
	screen := tcell.NewSimulationScreen("")
	require.NoError(t, screen.Init())
	defer screen.Fini()
	bar := NewProgressBar("my progress")
	bar.SetRect(0, 0, 80, 1)
	wbStyle := tcell.StyleDefault.Foreground(tcell.ColorWhite).Background(tcell.ColorBlack)
	bwStyle := tcell.StyleDefault.Foreground(tcell.ColorBlack).Background(tcell.ColorWhite)

	bar.SetTotal(10000)
	bar.SetCurrent(0)
	bar.Draw(screen)
	assertDrew(
		t, screen, 0,
		"                             my progress (0/10000)                              ",
		styleArray(
			wbStyle,
			80,
			nil,
		),
	)

	bar.SetCurrent(3000)
	bar.Draw(screen)
	assertDrew(
		t, screen, 0,
		"                            my progress (3000/10000)                            ",
		styleArray(
			wbStyle,
			80,
			map[tcell.Style][2]int{bwStyle: {0, 24}},
		),
	)

	bar.SetCurrent(5000)
	bar.Draw(screen)
	assertDrew(
		t, screen, 0,
		"                            my progress (5000/10000)                            ",
		styleArray(
			wbStyle,
			80,
			map[tcell.Style][2]int{bwStyle: {0, 40}},
		),
	)

	bar.SetCurrent(8000)
	bar.Draw(screen)
	assertDrew(
		t, screen, 0,
		"                            my progress (8000/10000)                            ",
		styleArray(
			wbStyle,
			80,
			map[tcell.Style][2]int{bwStyle: {0, 64}},
		),
	)

	bar.SetCurrent(10000)
	bar.Draw(screen)
	assertDrew(
		t, screen, 0,
		"                           my progress (10000/10000)                            ",
		styleArray(
			bwStyle,
			80,
			nil,
		),
	)
}
