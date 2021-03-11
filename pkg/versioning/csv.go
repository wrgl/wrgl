package versioning

import (
	"encoding/csv"
)

type CSVReader interface {
	Read() (record []string, err error)
}

// StoreLinesCSVReader work the same as regular csv reader
// however it also store every line that it read
type StoreLinesCSVReader struct {
	r     *csv.Reader
	lines [][]string
}

func NewStoreLinesCSVReader(r *csv.Reader) *StoreLinesCSVReader {
	return &StoreLinesCSVReader{
		r:     r,
		lines: [][]string{},
	}
}

func (r *StoreLinesCSVReader) Read() ([]string, error) {
	sl, err := r.r.Read()
	if err != nil {
		return nil, err
	}
	line := make([]string, len(sl))
	copy(line, sl)
	r.lines = append(r.lines, line)
	return sl, nil
}

func (r *StoreLinesCSVReader) Lines() [][]string {
	return r.lines[:]
}
