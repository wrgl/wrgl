// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"fmt"
	"strings"
)

type Example struct {
	Comment string
	Line    string
}

func CombineExamples(sl []Example) string {
	sb := &strings.Builder{}
	for i, ex := range sl {
		_, err := fmt.Fprintf(sb, "  # %s\n  %s", ex.Comment, ex.Line)
		if err != nil {
			panic(err)
		}
		if i < len(sl)-1 {
			_, err = sb.WriteString("\n\n")
			if err != nil {
				panic(err)
			}
		}
	}
	return sb.String()
}
