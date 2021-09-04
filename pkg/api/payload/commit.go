package payload

import "time"

type CommitResponse struct {
	Sum   *Hex `json:"sum,omitempty"`
	Table *Hex `json:"table,omitempty"`
}

type Commit struct {
	AuthorName    string             `json:"authorName,omitempty"`
	AuthorEmail   string             `json:"authorEmail,omitempty"`
	Message       string             `json:"message,omitempty"`
	Table         *Hex               `json:"table"`
	Time          time.Time          `json:"time,omitempty"`
	Parents       []*Hex             `json:"parents,omitempty"`
	ParentCommits map[string]*Commit `json:"parentCommits,omitempty"`
}

type GetCommitsResponse struct {
	Commits map[string]*Commit `json:"commits"`
}
