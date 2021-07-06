package widgets

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUsageBar(t *testing.T) {
	b := NewUsageBar([][2]string{
		{"n", "Next conflict"},
		{"r", "Mark row as resolved"},
		{"R", "Mark row as unresolved"},
		{"u", "Undo"},
		{"U", "Redo"},
		{"d", "Delete row"},
		{"D", "Delete column"},
		{"h", "Left"},
		{"j", "Down"},
		{"k", "Up"},
		{"l", "Right"},
		{"g", "Scroll to begin"},
		{"G", "Scroll to end"},
		{"Q", "Abort merge"},
		{"X", "Finish merge"},
	}, 2)

	b.printRows(120)
	assert.Equal(t, strings.Join([]string{
		" n  Next conflict   r  Mark row as resolved   R  Mark row as unresolved   u  Undo   U  Redo    d  Delete row     ",
		"",
		" D  Delete column   h  Left                   j  Down                     k  Up     l  Right   g  Scroll to begin",
		"",
		" G  Scroll to end   Q  Abort merge            X  Finish merge          ",
	}, "\n"), b.GetText(true))
	assert.Equal(t, 5, b.height)

	b.printRows(70)
	assert.Equal(t, strings.Join([]string{
		" n  Next conflict            r  Mark row as resolved",
		"",
		" R  Mark row as unresolved   u  Undo                ",
		"",
		" U  Redo                     d  Delete row          ",
		"",
		" D  Delete column            h  Left                ",
		"",
		" j  Down                     k  Up                  ",
		"",
		" l  Right                    g  Scroll to begin     ",
		"",
		" G  Scroll to end            Q  Abort merge         ",
		"",
		" X  Finish merge          ",
	}, "\n"), b.GetText(true))
	assert.Equal(t, 15, b.height)

	b.printRows(50)
	assert.Equal(t, strings.Join([]string{
		" n  Next conflict         ",
		"",
		" r  Mark row as resolved  ",
		"",
		" R  Mark row as unresolved",
		"",
		" u  Undo                  ",
		"",
		" U  Redo                  ",
		"",
		" d  Delete row            ",
		"",
		" D  Delete column         ",
		"",
		" h  Left                  ",
		"",
		" j  Down                  ",
		"",
		" k  Up                    ",
		"",
		" l  Right                 ",
		"",
		" g  Scroll to begin       ",
		"",
		" G  Scroll to end         ",
		"",
		" Q  Abort merge           ",
		"",
		" X  Finish merge          ",
	}, "\n"), b.GetText(true))
	assert.Equal(t, 29, b.height)

	b.printRows(10)
	assert.Equal(t, strings.Join([]string{
		" n  Next conflict       ",
		"",
		" r  Mark row as resolved",
		"",
		" R  Mark row as unresolved",
		"",
		" u  Undo                ",
		"",
		" U  Redo                ",
		"",
		" d  Delete row          ",
		"",
		" D  Delete column       ",
		"",
		" h  Left                ",
		"",
		" j  Down                ",
		"",
		" k  Up                  ",
		"",
		" l  Right               ",
		"",
		" g  Scroll to begin     ",
		"",
		" G  Scroll to end       ",
		"",
		" Q  Abort merge         ",
		"",
		" X  Finish merge        ",
	}, "\n"), b.GetText(true))
	assert.Equal(t, 29, b.height)
}
