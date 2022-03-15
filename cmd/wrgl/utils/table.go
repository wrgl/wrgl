// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"fmt"
	"io"
	"strings"
)

// PrintTable prints simple table from rows of text
func PrintTable(w io.Writer, rows [][]string, indent int) {
	widths := []int{}
	for _, row := range rows {
		for i, cell := range row {
			n := len(cell)
			if i >= len(widths) {
				widths = append(widths, n)
			} else if widths[i] < n {
				widths[i] = n
			}
		}
	}
	sum := 0
	for _, w := range widths {
		sum += w
	}
	for _, row := range rows {
		if indent > 0 {
			fmt.Fprint(w, strings.Repeat(" ", indent))
		}
		for i, cell := range row {
			fmt.Fprint(w, cell)
			fmt.Fprint(w, strings.Repeat(" ", widths[i]-len(cell)))
			if i == len(row)-1 {
				fmt.Fprint(w, "\n")
			} else {
				fmt.Fprint(w, " ")
			}
		}
	}
}
