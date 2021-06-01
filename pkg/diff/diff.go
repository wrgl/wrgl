// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"bytes"
	"io"
	"time"

	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/progress"
	"github.com/wrgl/core/pkg/table"
)

func strSliceEqual(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i, v := range s1 {
		if v != s2[i] {
			return false
		}
	}
	return true
}

func DiffTables(t1, t2 table.Store, progressPeriod time.Duration, errChan chan<- error, skipColumnChange, emitRowChangeWhenPKDiffer bool) (<-chan objects.Diff, progress.Tracker) {
	diffChan := make(chan objects.Diff)
	pt := progress.NewTracker(progressPeriod, 0)
	go func() {
		defer close(diffChan)

		pk1 := t1.PrimaryKey()
		cols1 := t1.Columns()
		pk2 := t2.PrimaryKey()
		cols2 := t2.Columns()
		pkEqual := strSliceEqual(pk1, pk2)
		colsEqual := strSliceEqual(cols1, cols2)

		if !skipColumnChange {
			cd := objects.CompareColumns([2][]string{cols2, pk2}, [2][]string{cols1, pk1})
			diffChan <- objects.Diff{
				Type:    objects.DTColumnChange,
				ColDiff: cd,
			}
		}

		if (!pkEqual && emitRowChangeWhenPKDiffer) || (pkEqual && (len(pk1) > 0 || colsEqual)) {
			err := diffRows(t1, t2, diffChan, pt)
			if err != nil {
				errChan <- err
				return
			}
		}
	}()
	return diffChan, pt
}

func diffRows(t1, t2 table.Store, diffChan chan<- objects.Diff, pt progress.Tracker) error {
	l1 := t1.NumRows()
	l2 := t2.NumRows()
	r1 := t1.NewRowHashReader(0, 0)
	pt.SetTotal(int64(l1 + l2))
	var current int64
loop1:
	for {
		k, v, err := r1.Read()
		if err == io.EOF {
			break loop1
		}
		if err != nil {
			return err
		}
		current++
		pt.SetCurrent(current)
		if w, ok := t2.GetRowHash(k); ok {
			if !bytes.Equal(v, w) {
				diffChan <- objects.Diff{
					Type:   objects.DTRow,
					PK:     k,
					OldSum: w,
					Sum:    v,
				}
			}
		} else {
			diffChan <- objects.Diff{
				Type: objects.DTRow,
				PK:   k,
				Sum:  v,
			}
		}
	}
	r2 := t2.NewRowHashReader(0, 0)
loop2:
	for {
		k, v, err := r2.Read()
		if err == io.EOF {
			break loop2
		}
		if err != nil {
			return err
		}
		current++
		pt.SetCurrent(current)
		if _, ok := t1.GetRowHash(k); !ok {
			diffChan <- objects.Diff{
				Type:   objects.DTRow,
				PK:     k,
				OldSum: v,
			}
		}
	}
	return nil
}
