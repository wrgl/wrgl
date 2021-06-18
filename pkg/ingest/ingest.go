// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

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

type Row struct {
	Index  int
	Record []string
}

func ReadColumns(file io.Reader, primaryKeys []string) (reader *csv.Reader, columns []string, primaryKeyIndices []uint32, err error) {
	reader = csv.NewReader(file)
	columns, err = reader.Read()
	if err != nil {
		err = fmt.Errorf("read CSV error: %v", err)
		return
	}

	primaryKeyIndices, err = slice.KeyIndices(columns, primaryKeys)
	if err != nil {
		err = fmt.Errorf("slice.KeyIndices: %v", err)
	}
	return
}

func printSpinner(out io.Writer, description string) chan bool {
	ticker := time.NewTicker(65 * time.Millisecond)
	done := make(chan bool)
	startTime := time.Now()
	maxLineWidth := utf8.RuneCountInString(description) + 2
	spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	go func() {
		neverPrint := true
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
				if neverPrint {
					neverPrint = false
					defer fmt.Fprintln(out)
				}
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

type Ingestor struct {
	primaryKeyIndices []uint32
	seed              uint64
	tb                *table.Builder
	errChan           chan error
	wg                sync.WaitGroup
	bar               *progressbar.ProgressBar
	numWorkers        int
	out               io.Writer
	rows              chan Row
}

func NewIngestor(tb *table.Builder, seed uint64, primaryKeyIndices []uint32, numWorkers int, out io.Writer) *Ingestor {
	return &Ingestor{
		primaryKeyIndices: primaryKeyIndices,
		seed:              seed,
		tb:                tb,
		errChan:           make(chan error, numWorkers+1),
		numWorkers:        numWorkers,
		out:               out,
	}
}

func (i *Ingestor) insertRows() {
	defer i.wg.Done()
	rh := NewRowHasher(i.primaryKeyIndices, i.seed)
	for r := range i.rows {
		pkHash, rowHash, rowContent, err := rh.Sum(r.Record)
		if err != nil {
			i.errChan <- err
			return
		}
		err = i.tb.InsertRow(r.Index, pkHash, rowHash, rowContent)
		if err != nil {
			i.errChan <- err
			return
		}
		err = i.bar.Add(1)
		if err != nil {
			i.errChan <- err
			return
		}
	}
}

func (i *Ingestor) SetRowsChan(rows chan Row) *Ingestor {
	i.rows = rows
	return i
}

func (i *Ingestor) ReadRowsFromCSVReader(reader *csv.Reader) *Ingestor {
	i.rows = make(chan Row, 1000)
	go func() {
		defer close(i.rows)
		n := 0
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				i.errChan <- fmt.Errorf("read CSV error: %v", err)
				return
			} else {
				i.rows <- Row{n, record}
				n++
			}
		}
	}()
	return i
}

func (i *Ingestor) Error() error {
	close(i.errChan)
	err, ok := <-i.errChan
	if ok {
		return err
	}
	return nil
}

func (i *Ingestor) Ingest() ([]byte, error) {
	i.bar = pbar(-1, fmt.Sprintf("Inserting rows using up to %d threads...", i.numWorkers), i.out)
	for j := 0; j < i.numWorkers; j++ {
		i.wg.Add(1)
		go i.insertRows()
	}
	i.wg.Wait()
	if err := i.Error(); err != nil {
		return nil, err
	}
	if err := i.bar.Finish(); err != nil {
		return nil, err
	}
	done := printSpinner(i.out, "Saving table...")
	defer close(done)
	return i.tb.SaveTable()
}
