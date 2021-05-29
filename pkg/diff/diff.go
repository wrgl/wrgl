// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"bytes"
	"io"
	"time"

	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/progress"
	"github.com/wrgl/core/pkg/slice"
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

func DiffTables(t1, t2 table.Store, progressPeriod time.Duration, errChan chan<- error) (<-chan objects.Diff, progress.Tracker) {
	diffChan := make(chan objects.Diff)
	pt := progress.NewTracker(progressPeriod, 0)
	go func() {
		defer close(diffChan)

		_, added, removed := slice.CompareStringSlices(t1.Columns(), t2.Columns())
		if len(added) > 0 {
			diffChan <- objects.Diff{
				Type:    objects.DTColumnAdd,
				Columns: added,
			}
		}
		if len(removed) > 0 {
			diffChan <- objects.Diff{
				Type:    objects.DTColumnRem,
				Columns: removed,
			}
		}

		sl1 := t1.PrimaryKey()
		sl2 := t2.PrimaryKey()
		if len(sl1) == 0 && len(sl2) == 0 && (len(added) > 0 || len(removed) > 0) {
			return
		}
		if !strSliceEqual(sl1, sl2) {
			diffChan <- objects.Diff{
				Type:    objects.DTPKChange,
				Columns: sl2,
			}
		} else {
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
