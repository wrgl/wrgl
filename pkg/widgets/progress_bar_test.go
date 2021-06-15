package widgets

import (
	"fmt"
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var colorMap = map[tcell.Color]string{
	tcell.ColorWhite:      "ColorWhite",
	tcell.ColorBlack:      "ColorBlack",
	tcell.ColorGreen:      "ColorGreen",
	tcell.ColorRed:        "ColorRed",
	tcell.ColorYellow:     "ColorYellow",
	tcell.ColorDarkRed:    "ColorDarkRed",
	tcell.ColorSlateGray:  "ColorSlateGray",
	tcell.ColorAquaMarine: "ColorAquaMarine",
}

func styleMatrix(baseStyle tcell.Style, width, height int, styleRanges map[tcell.Style][][3]int) [][]tcell.Style {
	sl := make([][]tcell.Style, height)
	for i := 0; i < height; i++ {
		sl[i] = make([]tcell.Style, width)
		for j := 0; j < width; j++ {
			sl[i][j] = baseStyle
		}
	}
	for st, occurrences := range styleRanges {
		for _, ints := range occurrences {
			row, start, end := ints[0], ints[1], ints[2]
			for i := start; i < end; i++ {
				sl[row][i] = st
			}
		}
	}
	return sl
}

func assertStyleEqual(t *testing.T, a, b tcell.Style, msgAndArgs ...interface{}) {
	t.Helper()
	fga, bga, atta := a.Decompose()
	fgb, bgb, attb := b.Decompose()
	msg := ""
	if len(msgAndArgs) > 0 {
		msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
	}
	assert.Equal(t, colorMap[fga], colorMap[fgb], "foreground color not equal at %s", msg)
	assert.Equal(t, colorMap[bga], colorMap[bgb], "background color not equal at %s", msg)
	assert.Equal(t, atta, attb, "attributes not equal at %s", msg)
}

func assertDrew(t *testing.T, screen tcell.SimulationScreen, startRow, endRow int, text []string, styles [][]tcell.Style) {
	t.Helper()
	screen.Show()
	cells, width, height := screen.GetContents()
	require.GreaterOrEqual(t, startRow, 0)
	require.GreaterOrEqual(t, height, endRow)
	require.Equal(t, endRow-startRow, len(text))
	for i := startRow; i < endRow; i++ {
		b := []byte{}
		for j := 0; j < width; j++ {
			b = append(b, cells[i*width+j].Bytes...)
		}
		assert.Equal(t, text[i-startRow], string(b), "row:%d", i)
		for j := 0; j < width; j++ {
			assertStyleEqual(t, styles[i-startRow][j], cells[i*width+j].Style, "row:%d col:%d", i, j)
		}
	}
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
		t, screen, 0, 1,
		[]string{"                             my progress (0/10000)                              "},
		styleMatrix(
			wbStyle,
			80,
			1,
			nil,
		),
	)

	bar.SetCurrent(3000)
	bar.Draw(screen)
	assertDrew(
		t, screen, 0, 1,
		[]string{"                            my progress (3000/10000)                            "},
		styleMatrix(
			wbStyle,
			80,
			1,
			map[tcell.Style][][3]int{bwStyle: {{0, 0, 24}}},
		),
	)

	bar.SetCurrent(5000)
	bar.Draw(screen)
	assertDrew(
		t, screen, 0, 1,
		[]string{"                            my progress (5000/10000)                            "},
		styleMatrix(
			wbStyle,
			80,
			1,
			map[tcell.Style][][3]int{bwStyle: {{0, 0, 40}}},
		),
	)

	bar.SetCurrent(8000)
	bar.Draw(screen)
	assertDrew(
		t, screen, 0, 1,
		[]string{"                            my progress (8000/10000)                            "},
		styleMatrix(
			wbStyle,
			80,
			1,
			map[tcell.Style][][3]int{bwStyle: {{0, 0, 64}}},
		),
	)

	bar.SetCurrent(10000)
	bar.Draw(screen)
	assertDrew(
		t, screen, 0, 1,
		[]string{"                           my progress (10000/10000)                            "},
		styleMatrix(
			bwStyle,
			80,
			1,
			nil,
		),
	)
}
