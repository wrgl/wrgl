// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package payload

type CSVLocation struct {
	StartLine int `json:"startLine"` // Line where the record starts
	Line      int `json:"line"`      // Line where the error occurred
	Column    int `json:"column"`    // Column (1-based byte index) where the error occurred
}

func (a *CSVLocation) Equal(b *CSVLocation) bool {
	if a == nil {
		return b == nil
	} else if b == nil {
		return false
	}
	return a.StartLine == b.StartLine && a.Line == b.Line && a.Column == b.Column
}

type Error struct {
	// Message is always present if there's an error. Otherwise this object
	// will be empty.
	Message string `json:"message,omitempty"` // The actual error

	// CSV appears when the error is a CSV parsing error
	CSV *CSVLocation `json:"csv,omitempty"`
}

func (a *Error) Equal(b *Error) bool {
	if a == nil {
		return b == nil
	} else if b == nil {
		return false
	}
	return a.Message == b.Message && a.CSV.Equal(b.CSV)
}
