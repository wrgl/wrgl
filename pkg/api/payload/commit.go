package payload

import "time"

type CommitResponse struct {
	Sum   *Hex `json:"sum,omitempty"`
	Table *Hex `json:"table,omitempty"`
}

type Table struct {
	Sum       *Hex     `json:"sum,omitempty"`
	Columns   []string `json:"columns"`
	PK        []uint32 `json:"pk,omitempty"`
	RowsCount uint32   `json:"rowsCount"`
}

type Commit struct {
	AuthorName    string             `json:"authorName,omitempty"`
	AuthorEmail   string             `json:"authorEmail,omitempty"`
	Message       string             `json:"message,omitempty"`
	Table         *Table             `json:"table,omitempty"`
	Time          time.Time          `json:"time,omitempty"`
	Parents       []*Hex             `json:"parents,omitempty"`
	ParentCommits map[string]*Commit `json:"parentCommits,omitempty"`
}

type GetCommitsResponse struct {
	Root Commit `json:"root"`
}
