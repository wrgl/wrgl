package payload

type CSVLocation struct {
	StartLine int `json:"startLine"` // Line where the record starts
	Line      int `json:"line"`      // Line where the error occurred
	Column    int `json:"column"`    // Column (1-based byte index) where the error occurred
}

type Error struct {
	// Message is always present if there's an error. Otherwise this object
	// will be empty.
	Message string `json:"message,omitempty"` // The actual error

	// CSV appears when the error is a CSV parsing error
	CSV *CSVLocation `json:"csv,omitempty"`
}
