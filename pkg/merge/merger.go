// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package merge

import (
	"fmt"
	"reflect"
	"time"

	"github.com/wrgl/core/pkg/diff"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/progress"
	"github.com/wrgl/core/pkg/table"
)

type Merger struct {
	db             kv.DB
	fs             kv.FileStore
	errChan        chan error
	progressPeriod time.Duration
	Progress       progress.Tracker
	baseT          table.Store
	otherTs        []table.Store
	collector      *RowCollector
}

func NewMerger(db kv.DB, fs kv.FileStore, collector *RowCollector, progressPeriod time.Duration, baseT table.Store, otherTs ...table.Store) (m *Merger, err error) {
	m = &Merger{
		db:             db,
		fs:             fs,
		errChan:        make(chan error, len(otherTs)),
		progressPeriod: progressPeriod,
		baseT:          baseT,
		otherTs:        otherTs,
		collector:      collector,
	}
	return
}

func (m *Merger) Error() error {
	close(m.errChan)
	err, ok := <-m.errChan
	if !ok {
		return nil
	}
	return err
}

func strSliceEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i, s := range left {
		if s != right[i] {
			return false
		}
	}
	return true
}

func (m *Merger) mergeTables(colDiff *objects.ColDiff, mergeChan chan<- Merge, errChan chan<- error, diffChans ...<-chan objects.Diff) {
	var (
		n        = len(diffChans)
		cases    = make([]reflect.SelectCase, n)
		closed   = make([]bool, n)
		merges   = map[string]*Merge{}
		counter  = map[string]int{}
		resolver = NewRowResolver(m.db, colDiff)
	)

	mergeChan <- Merge{
		ColDiff: colDiff,
	}
	defer close(mergeChan)

	for i, ch := range diffChans {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}

	for {
		chosen, recv, ok := reflect.Select(cases)
		if !ok {
			closed[chosen] = true
			allClosed := true
			for _, b := range closed {
				if !b {
					allClosed = false
					break
				}
			}
			if allClosed {
				break
			}
			continue
		}
		d := recv.Interface().(objects.Diff)
		switch d.Type {
		case objects.DTRow:
			pkSum := string(d.PK)
			if m, ok := merges[pkSum]; !ok {
				merges[pkSum] = &Merge{
					PK:     d.PK,
					Base:   d.OldSum,
					Others: make([][]byte, n),
				}
				merges[pkSum].Others[chosen] = d.Sum
				counter[pkSum] = 1
			} else {
				m.Others[chosen] = d.Sum
				counter[pkSum]++
				if counter[pkSum] == n {
					err := resolver.Resolve(merges[pkSum])
					if err != nil {
						errChan <- fmt.Errorf("resolve error: %v", err)
						return
					}
					mergeChan <- *merges[pkSum]
					delete(merges, pkSum)
					delete(counter, pkSum)
				}
			}
		}
	}
	for _, m := range merges {
		err := resolver.Resolve(m)
		if err != nil {
			errChan <- fmt.Errorf("resolve error: %v", err)
			return
		}
		mergeChan <- *m
	}
}

func (m *Merger) Start() (ch <-chan Merge, err error) {
	n := len(m.otherTs)
	var pk []string
	for _, t := range m.otherTs {
		if pk == nil {
			pk = t.PrimaryKey()
		} else if !strSliceEqual(pk, t.PrimaryKey()) {
			return nil, fmt.Errorf("can't merge: primary key differs between versions")
		}
	}
	mergeChan := make(chan Merge)
	diffs := make([]<-chan objects.Diff, n)
	progs := make([]progress.Tracker, n)
	cols := make([][2][]string, n)
	for i, t := range m.otherTs {
		diffChan, progTracker := diff.DiffTables(t, m.baseT, m.progressPeriod*time.Duration(n), m.errChan, true, false)
		diffs[i] = diffChan
		progs[i] = progTracker
		cols[i] = [2][]string{t.Columns(), t.PrimaryKey()}
	}
	colDiff := objects.CompareColumns([2][]string{m.baseT.Columns(), m.baseT.PrimaryKey()}, cols...)
	m.Progress = progress.JoinTrackers(progs...)
	go m.mergeTables(colDiff, mergeChan, m.errChan, diffs...)
	return m.collector.CollectResolvedRow(m.errChan, mergeChan), nil
}

func (m *Merger) SaveResolvedRow(pk []byte, row []string) error {
	return m.collector.SaveResolvedRow(pk, row)
}

func (m *Merger) SortedRows() (<-chan []string, error) {
	m.errChan = make(chan error, 1)
	return m.collector.SortedRows(m.errChan)
}

func (m *Merger) Columns() []string {
	return m.collector.Columns()
}

func (m *Merger) PK() []string {
	return m.collector.PK()
}

func (m *Merger) Close() error {
	return m.collector.Close()
}
