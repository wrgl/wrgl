// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package merge

import (
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/wrgl/core/pkg/diff"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/progress"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
)

type Merger struct {
	db             kv.DB
	fs             kv.FileStore
	errChan        chan error
	progressPeriod time.Duration
	Progress       progress.Tracker
	commitSums     [][]byte
	tableSums      [][]byte
	baseTable      []byte
	collector      *RowCollector
}

func NewMerger(db kv.DB, fs kv.FileStore, collector *RowCollector, progressPeriod time.Duration, commits ...[]byte) (m *Merger, err error) {
	n := len(commits)
	m = &Merger{
		db:             db,
		fs:             fs,
		errChan:        make(chan error, n),
		progressPeriod: progressPeriod,
		commitSums:     make([][]byte, n),
		tableSums:      make([][]byte, n),
		collector:      collector,
	}
	err = m.saveCommitSums(commits...)
	if err != nil {
		return nil, err
	}
	err = m.seekCommonAncestor()
	if err != nil {
		return nil, err
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

func (m *Merger) saveCommitSums(commits ...[]byte) error {
	for i, sum := range commits {
		m.commitSums[i] = sum
		com, err := versioning.GetCommit(m.db, sum)
		if err != nil {
			return err
		}
		m.tableSums[i] = com.Table
	}
	return nil
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

func (m *Merger) seekCommonAncestor() (err error) {
	n := len(m.commitSums)
	qs := make([]*versioning.CommitsQueue, n)
	bases := make([][]byte, n)
	for i, sum := range m.commitSums {
		qs[i], err = versioning.NewCommitsQueue(m.db, [][]byte{sum})
		if err != nil {
			return
		}
		bases[i] = sum
	}
	for {
		for i := len(bases) - 1; i >= 0; i-- {
			for j := len(bases) - 1; j >= 0; j-- {
				if i == j {
					continue
				}
				if qs[j].Seen(bases[i]) {
					// remove j element
					copy(bases[j:], bases[j+1:])
					bases = bases[:len(bases)-1]
					copy(qs[j:], qs[j+1:])
					qs = qs[:len(qs)-1]
					if i > j {
						i--
					}
				}
			}
		}
		if len(bases) == 1 {
			break
		}
		eofs := 0
		for i, q := range qs {
			bases[i], _, err = q.PopInsertParents()
			if err == io.EOF {
				eofs++
			} else if err != nil {
				return err
			}
		}
		if eofs == len(qs) {
			return fmt.Errorf("common ancestor commit not found")
		}
	}
	com, err := versioning.GetCommit(m.db, bases[0])
	if err != nil {
		return
	}
	m.baseTable = com.Table
	return nil
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
						errChan <- err
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
			errChan <- err
			return
		}
		mergeChan <- *m
	}
}

func (m *Merger) Start() (ch <-chan Merge, err error) {
	baseT, err := table.ReadTable(m.db, m.fs, m.baseTable)
	if err != nil {
		return
	}
	n := len(m.tableSums)
	otherTs := make([]table.Store, n)
	var pk []string
	for i, sum := range m.tableSums {
		t, err := table.ReadTable(m.db, m.fs, sum)
		if err != nil {
			return nil, err
		}
		if pk == nil {
			pk = t.PrimaryKey()
		} else if !strSliceEqual(pk, t.PrimaryKey()) {
			return nil, fmt.Errorf("can't merge: primary key differs between versions")
		}
		otherTs[i] = t
	}
	mergeChan := make(chan Merge)
	diffs := make([]<-chan objects.Diff, n)
	progs := make([]progress.Tracker, n)
	cols := make([][2][]string, n)
	for i, t := range otherTs {
		diffChan, progTracker := diff.DiffTables(t, baseT, m.progressPeriod*2, m.errChan, true, false)
		diffs[i] = diffChan
		progs[i] = progTracker
		cols[i] = [2][]string{t.Columns(), t.PrimaryKey()}
	}
	colDiff := objects.CompareColumns([2][]string{baseT.Columns(), baseT.PrimaryKey()}, cols...)
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
