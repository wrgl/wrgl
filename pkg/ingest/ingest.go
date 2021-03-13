package ingest

import (
	"encoding/csv"
	"fmt"
	"io"
	"sync"

	"github.com/wrgl/core/pkg/slice"
	"github.com/wrgl/core/pkg/table"
)

type row struct {
	Index  int
	Record []string
}

func insertRows(primaryKeyIndices []int, seed uint64, ts table.Store, rows <-chan row, errChan chan<- error, wg *sync.WaitGroup) {
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

func Ingest(seed uint64, numWorkers int, reader *csv.Reader, primaryKeyIndices []int, ts table.Store) (string, error) {
	errChan := make(chan error)
	rows := make(chan row, 1000)
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go insertRows(primaryKeyIndices, seed, ts, rows, errChan, &wg)
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
	return ts.Save()
}
