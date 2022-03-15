// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package payload

type BlockFormat string

const (
	BlockFormatCSV    BlockFormat = "csv"
	BlockFormatBinary BlockFormat = "binary"
)

type GetTableResponse struct {
	Columns   []string `json:"columns,omitempty"`
	PK        []uint32 `json:"pk,omitempty"`
	RowsCount uint32   `json:"rowsCount"`
}
