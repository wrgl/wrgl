// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package merge

import "github.com/wrgl/wrgl/pkg/diff"

type Merge struct {
	ColDiff        *diff.ColDiff
	PK             []byte
	Base           []byte
	BaseOffset     uint32
	Others         [][]byte
	OtherOffsets   []uint32
	ResolvedRow    []string
	Resolved       bool
	UnresolvedCols map[uint32]struct{}
}
