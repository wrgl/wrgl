package versioning

import (
	"bytes"
	"encoding/csv"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreLinesCSVReader(t *testing.T) {
	data := strings.Join([]string{
		"a,c",
		"1,4",
		"2,5",
		"",
	}, "\n")

	r := csv.NewReader(bytes.NewBuffer([]byte(data)))
	r.ReuseRecord = true

	sr := NewStoreLinesCSVReader(r)
	rows := [][]string{}
	for {
		row, err := sr.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		line := make([]string, len(row))
		copy(line, row)
		rows = append(rows, line)
	}
	assert.Equal(t, [][]string{
		{"a", "c"},
		{"1", "4"},
		{"2", "5"},
	}, rows)

	assert.Equal(t, rows, sr.Lines())
}
