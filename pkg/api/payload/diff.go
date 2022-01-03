// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package payload

import diffprof "github.com/wrgl/wrgl/pkg/diff/prof"

type RowDiff struct {
	Offset1 *uint32 `json:"off1,omitempty"`
	Offset2 *uint32 `json:"off2,omitempty"`
}

type DiffResponse struct {
	TableSum    *Hex                       `json:"tableSum"`
	OldTableSum *Hex                       `json:"oldTableSum"`
	OldPK       []uint32                   `json:"oldPK,omitempty"`
	PK          []uint32                   `json:"pk,omitempty"`
	OldColumns  []string                   `json:"oldColumns"`
	Columns     []string                   `json:"columns"`
	RowDiff     []*RowDiff                 `json:"rowDiff"`
	DataProfile *diffprof.TableProfileDiff `json:"dataProfile,omitempty"`
}
