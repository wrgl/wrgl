package payload

import "time"

type CommitResponse struct {
	Sum *Hex `json:"sum,omitempty"`
}

type GetCommitResponse struct {
	AuthorName  string    `json:"authorName,omitempty"`
	AuthorEmail string    `json:"authorEmail,omitempty"`
	Message     string    `json:"message,omitempty"`
	Table       *Hex      `json:"table"`
	Time        time.Time `json:"time,omitempty"`
	Parents     []*Hex    `json:"parents,omitempty"`
}
