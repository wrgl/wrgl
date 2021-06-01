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
	"github.com/wrgl/core/pkg/versioning"
)

type Merge struct {
	ColDiff     *objects.ColDiff
	Base        []byte
	Others      [][]byte
	ResolvedRow []string
	Resolved    bool
}

func openTable(db kv.DB, fs kv.FileStore, commitSum []byte) (table.Store, error) {
	c, err := versioning.GetCommit(db, commitSum)
	if err != nil {
		return nil, err
	}
	return table.ReadTable(db, fs, c.Table)
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

func MergeCommits(db kv.DB, fs kv.FileStore, progressPeriod time.Duration, errChan chan<- error, base []byte, others ...[]byte) (<-chan Merge, progress.Tracker, error) {
	baseT, err := openTable(db, fs, base)
	if err != nil {
		return nil, nil, err
	}
	n := len(others)
	otherTs := make([]table.Store, n)
	var pk []string
	for i, sum := range others {
		t, err := openTable(db, fs, sum)
		if err != nil {
			return nil, nil, err
		}
		if pk == nil {
			pk = t.PrimaryKey()
		} else if !strSliceEqual(pk, t.PrimaryKey()) {
			return nil, nil, fmt.Errorf("can't merge: primary key differs between versions")
		}
		otherTs[i] = t
	}
	mergeChan := make(chan Merge)
	diffs := make([]<-chan objects.Diff, n)
	progs := make([]progress.Tracker, n)
	cols := make([][2][]string, n)
	for i, t := range otherTs {
		// TODO: add an option that makes DiffTable emit row changes even if PK has changed
		// TODO: add an option that disallow DiffTable to emit DTColumnChange event
		diffChan, progTracker := diff.DiffTables(t, baseT, progressPeriod*2, errChan, true, false)
		diffs[i] = diffChan
		progs[i] = progTracker
		cols[i] = [2][]string{t.Columns(), t.PrimaryKey()}
	}
	colDiff := objects.CompareColumns([2][]string{baseT.Columns(), baseT.PrimaryKey()}, cols...)
	joinedProg := progress.JoinTrackers(progs...)
	go mergeTables(db, colDiff, mergeChan, errChan, diffs...)
	return mergeChan, joinedProg, nil
}

func mergeTables(db kv.DB, colDiff *objects.ColDiff, mergeChan chan<- Merge, errChan chan<- error, diffChans ...<-chan objects.Diff) {
	var (
		n        = len(diffChans)
		cases    = make([]reflect.SelectCase, n)
		closed   = make([]bool, n)
		merges   = map[string]*Merge{}
		counter  = map[string]int{}
		resolver = NewResolver(db, colDiff)
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
