// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package payload

import "time"

type CommitResponse struct {
	Sum   *Hex `json:"sum,omitempty"`
	Table *Hex `json:"table,omitempty"`
}

type Table struct {
	Sum       *Hex     `json:"sum,omitempty"`
	Columns   []string `json:"columns,omitempty"`
	PK        []uint32 `json:"pk,omitempty"`
	RowsCount uint32   `json:"rowsCount,omitempty"`
	Exist     bool     `json:"exist"`
}

type Commit struct {
	Sum           *Hex               `json:"sum,omitempty"`
	AuthorName    string             `json:"authorName,omitempty"`
	AuthorEmail   string             `json:"authorEmail,omitempty"`
	Message       string             `json:"message,omitempty"`
	Table         *Table             `json:"table,omitempty"`
	Time          time.Time          `json:"time,omitempty"`
	Parents       []*Hex             `json:"parents,omitempty"`
	ParentCommits map[string]*Commit `json:"parentCommits,omitempty"`
}

type GetCommitsResponse struct {
	Sum  *Hex   `json:"sum,omitempty"`
	Root Commit `json:"root"`
}
