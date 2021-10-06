// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package merge

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	"github.com/wrgl/wrgl/pkg/diff"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/progress"
	"github.com/wrgl/wrgl/pkg/sorter"
)

type Merger struct {
	db             objects.Store
	errChan        chan error
	progressPeriod time.Duration
	Progress       progress.Tracker
	baseT          *objects.Table
	otherTs        []*objects.Table
	baseSum        []byte
	otherSums      [][]byte
	buf            *diff.BlockBuffer
	collector      *RowCollector
}

func NewMerger(db objects.Store, collector *RowCollector, buf *diff.BlockBuffer, progressPeriod time.Duration, baseT *objects.Table, otherTs []*objects.Table, baseSum []byte, otherSums [][]byte) (m *Merger, err error) {
	m = &Merger{
		db:             db,
		errChan:        make(chan error, len(otherTs)),
		progressPeriod: progressPeriod,
		baseT:          baseT,
		otherTs:        otherTs,
		baseSum:        baseSum,
		otherSums:      otherSums,
		collector:      collector,
		buf:            buf,
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

func (m *Merger) mergeTables(colDiff *diff.ColDiff, mergeChan chan<- *Merge, errChan chan<- error, diffChans ...<-chan *objects.Diff) {
	var (
		n        = len(diffChans)
		cases    = make([]reflect.SelectCase, n)
		closed   = make([]bool, n)
		merges   = map[string]*Merge{}
		counter  = map[string]int{}
		resolver = NewRowResolver(m.db, colDiff, m.buf)
	)

	mergeChan <- &Merge{
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
		d := recv.Interface().(*objects.Diff)
		pkSum := string(d.PK)
		if m, ok := merges[pkSum]; !ok {
			merges[pkSum] = &Merge{
				PK:           d.PK,
				Base:         d.OldSum,
				BaseOffset:   d.OldOffset,
				Others:       make([][]byte, n),
				OtherOffsets: make([]uint32, n),
			}
			merges[pkSum].Others[chosen] = d.Sum
			merges[pkSum].OtherOffsets[chosen] = d.Offset
			counter[pkSum] = 1
		} else {
			m.Others[chosen] = d.Sum
			m.OtherOffsets[chosen] = d.Offset
			counter[pkSum]++
		}
	}
	for _, obj := range merges {
		if obj.Base != nil {
			noChanges := true
			for _, b := range obj.Others {
				if !bytes.Equal(b, obj.Base) {
					noChanges = false
					break
				}
			}
			if noChanges {
				continue
			}
		}
		err := resolver.Resolve(obj)
		if err != nil {
			errChan <- fmt.Errorf("resolve error: %v", err)
			return
		}
		mergeChan <- obj
	}
}

func (m *Merger) Start() (ch <-chan *Merge, err error) {
	n := len(m.otherTs)
	var pk []string
	for _, t := range m.otherTs {
		if pk == nil {
			pk = t.PrimaryKey()
		} else if !strSliceEqual(pk, t.PrimaryKey()) {
			return nil, fmt.Errorf("can't merge: primary key differs between versions")
		}
	}
	mergeChan := make(chan *Merge)
	diffs := make([]<-chan *objects.Diff, n)
	progs := make([]progress.Tracker, n)
	cols := make([][2][]string, n)
	baseIdx, err := objects.GetTableIndex(m.db, m.baseSum)
	if err != nil {
		return nil, err
	}
	for i, t := range m.otherTs {
		idx, err := objects.GetTableIndex(m.db, m.otherSums[i])
		if err != nil {
			return nil, err
		}
		diffChan, progTracker := diff.DiffTables(
			m.db, m.db, t, m.baseT, idx, baseIdx, m.errChan,
			diff.WithProgressInterval(m.progressPeriod*time.Duration(n)),
			diff.WithEmitUnchangedRow(),
		)
		diffs[i] = diffChan
		progs[i] = progTracker
		cols[i] = [2][]string{t.Columns, t.PrimaryKey()}
	}
	colDiff := diff.CompareColumns([2][]string{m.baseT.Columns, m.baseT.PrimaryKey()}, cols...)
	m.Progress = progress.JoinTrackers(progs...)
	go m.mergeTables(colDiff, mergeChan, m.errChan, diffs...)
	return m.collector.CollectResolvedRow(m.errChan, mergeChan), nil
}

func (m *Merger) SaveResolvedRow(pk []byte, row []string) error {
	return m.collector.SaveResolvedRow(pk, row)
}

func (m *Merger) SortedBlocks(removedCols map[int]struct{}) (<-chan *sorter.Block, uint32, error) {
	m.errChan = make(chan error, 1)
	return m.collector.SortedBlocks(removedCols, m.errChan)
}

func (m *Merger) SortedRows(removedCols map[int]struct{}) (<-chan *sorter.Rows, uint32, error) {
	m.errChan = make(chan error, 1)
	return m.collector.SortedRows(removedCols, m.errChan)
}

func (m *Merger) Columns(removedCols map[int]struct{}) []string {
	return m.collector.Columns(removedCols)
}

func (m *Merger) PK() []string {
	return m.collector.PK()
}

func (m *Merger) Close() error {
	return m.collector.Close()
}
