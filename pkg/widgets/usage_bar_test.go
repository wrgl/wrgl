package widgets

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUsageBar(t *testing.T) {
	b := NewUsageBar([][2]string{
		{"h", "Left"},
		{"j", "Down"},
		{"g", "Scroll to begin"},
		{"G", "Scroll to end"},
		{"k", "Up"},
		{"l", "Right"},
	}, 2)

	b.printRows(120)
	assert.Equal(t, " h  Left   j  Down   g  Scroll to begin   G  Scroll to end   k  Up   l  Right", b.GetText(true))
	assert.Equal(t, 1, b.height)

	b.printRows(70)
	assert.Equal(t, strings.Join([]string{
		" h  Left    j  Down   g  Scroll to begin   G  Scroll to end   k  Up",
		"",
		" l  Right",
	}, "\n"), b.GetText(true))
	assert.Equal(t, 3, b.height)

	b.printRows(60)
	assert.Equal(t, strings.Join([]string{
		" h  Left   j  Down    g  Scroll to begin   G  Scroll to end",
		"",
		" k  Up     l  Right",
	}, "\n"), b.GetText(true))
	assert.Equal(t, 3, b.height)

	b.printRows(50)
	assert.Equal(t, strings.Join([]string{
		" h  Left            j  Down   g  Scroll to begin",
		"",
		" G  Scroll to end   k  Up     l  Right          ",
	}, "\n"), b.GetText(true))
	assert.Equal(t, 3, b.height)

	b.printRows(45)
	assert.Equal(t, strings.Join([]string{
		" h  Left              j  Down         ",
		"",
		" g  Scroll to begin   G  Scroll to end",
		"",
		" k  Up                l  Right        ",
	}, "\n"), b.GetText(true))
	assert.Equal(t, 5, b.height)

	b.printRows(30)
	assert.Equal(t, strings.Join([]string{
		" h  Left           ",
		"",
		" j  Down           ",
		"",
		" g  Scroll to begin",
		"",
		" G  Scroll to end  ",
		"",
		" k  Up             ",
		"",
		" l  Right          ",
	}, "\n"), b.GetText(true))
	assert.Equal(t, 11, b.height)

	b.printRows(10)
	assert.Equal(t, strings.Join([]string{
		" h  Left           ",
		"",
		" j  Down           ",
		"",
		" g  Scroll to begin",
		"",
		" G  Scroll to end  ",
		"",
		" k  Up             ",
		"",
		" l  Right          ",
	}, "\n"), b.GetText(true))
	assert.Equal(t, 11, b.height)
}
