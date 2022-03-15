// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrintTable(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	for i, c := range []struct {
		Table  [][]string
		Indent int
		Text   string
	}{
		{
			[][]string{
				{"abc", "d", "q", "e"},
				{"ab", "c", "defg"},
				{"a", "b"},
			},
			0,
			strings.Join([]string{
				"abc d q    e",
				"ab  c defg",
				"a   b",
				"",
			}, "\n"),
		},
		{
			[][]string{
				{"abc", "d", "q", "e"},
				{"ab", "c", "defg"},
				{"a", "b"},
			},
			4,
			strings.Join([]string{
				"    abc d q    e",
				"    ab  c defg",
				"    a   b",
				"",
			}, "\n"),
		},
	} {
		buf.Reset()
		PrintTable(buf, c.Table, c.Indent)
		assert.Equal(t, c.Text, buf.String(), "case %d", i)
	}
}
