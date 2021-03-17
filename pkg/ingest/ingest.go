package ingest

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/schollz/progressbar/v3"
	"github.com/wrgl/core/pkg/slice"
	"github.com/wrgl/core/pkg/table"
)

type row struct {
	Index  int
	Record []string
}

func insertRows(primaryKeyIndices []int, seed uint64, ts table.Store, rows <-chan row, errChan chan<- error, wg *sync.WaitGroup, bar *progressbar.ProgressBar) {
	defer wg.Done()
	rh := NewRowHasher(primaryKeyIndices, seed)
	for r := range rows {
		pkHash, rowHash, rowContent, err := rh.Sum(r.Record)
		if err != nil {
			errChan <- err
			return
		}
		err = ts.InsertRow(r.Index, pkHash, rowHash, rowContent)
		if err != nil {
			errChan <- err
			return
		}
		err = bar.Add(1)
		if err != nil {
			errChan <- err
			return
		}
	}
}

func ReadColumns(file io.Reader, primaryKeys []string) (reader *csv.Reader, columns []string, primaryKeyIndices []int, err error) {
	reader = csv.NewReader(file)
	columns, err = reader.Read()
	if err != nil {
		err = fmt.Errorf("csv.Reader.Read: %v", err)
		return
	}

	primaryKeyIndices, err = slice.KeyIndices(columns, primaryKeys)
	if err != nil {
		err = fmt.Errorf("slice.KeyIndices: %v", err)
	}
	return
}

func printSpinner(out io.Writer, description string) chan bool {
	ticker := time.NewTicker(500 * time.Millisecond)
	done := make(chan bool)
	startTime := time.Now()
	maxLineWidth := utf8.RuneCountInString(description) + 2
	spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				// clear current line
				fmt.Fprintf(out, "\r%s\r", strings.Repeat(" ", maxLineWidth))
				// print spinner and description
				fmt.Fprintf(out, "%s %s",
					spinner[int(math.Round(math.Mod(float64(time.Since(startTime).Milliseconds()/100), float64(len(spinner)))))],
					description)
			}
		}
	}()
	return done
}

func pbar(max int64, desc string, out io.Writer) *progressbar.ProgressBar {
	bar := progressbar.NewOptions64(
		max,
		progressbar.OptionSetDescription(desc),
		progressbar.OptionSetWriter(out),
		progressbar.OptionSetWidth(10),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
	)
	bar.RenderBlank()
	return bar
}

func Ingest(seed uint64, numWorkers int, reader *csv.Reader, primaryKeyIndices []int, ts table.Store, out io.Writer) (string, error) {
	errChan := make(chan error)
	rows := make(chan row, 1000)
	var wg sync.WaitGroup
	bar := pbar(-1, fmt.Sprintf("Inserting rows using up to %d threads...", numWorkers), out)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go insertRows(primaryKeyIndices, seed, ts, rows, errChan, &wg, bar)
	}

	n := 0
outer:
	for {
		select {
		case err := <-errChan:
			return "", err
		default:
			record, err := reader.Read()
			if err == io.EOF {
				close(rows)
				break outer
			} else if err != nil {
				return "", fmt.Errorf("csv.Reader.Read: %v", err)
			} else {
				rows <- row{n, record}
				n++
			}
		}
	}

	wg.Wait()
	done := printSpinner(out, "Saving table...")
	defer close(done)
	return ts.Save()
}
