// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"bytes"
	"time"

	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/progress"
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

func uintSliceEqual(s1, s2 []uint32) bool {
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

func DiffTables(db1, db2 objects.Store, tbl1, tbl2 *objects.Table, tblIdx1, tblIdx2 [][]string, progressPeriod time.Duration, errChan chan<- error, emitUnchangedRow bool) (<-chan *objects.Diff, progress.Tracker) {
	diffChan := make(chan *objects.Diff)
	pt := progress.NewTracker(progressPeriod, 0)
	go func() {
		defer close(diffChan)

		pkEqual := strSliceEqual(tbl1.PrimaryKey(), tbl2.PrimaryKey())
		colsEqual := strSliceEqual(tbl1.Columns, tbl2.Columns)

		if pkEqual && (len(tbl1.PK) > 0 || colsEqual) {
			err := diffRows(db1, db2, tbl1, tbl2, tblIdx1, tblIdx2, diffChan, pt, colsEqual, emitUnchangedRow)
			if err != nil {
				errChan <- err
				return
			}
		}
	}()
	return diffChan, pt
}

func diffRows(db1, db2 objects.Store, tbl1, tbl2 *objects.Table, tblIdx1, tblIdx2 [][]string, diffChan chan<- *objects.Diff, pt progress.Tracker, colsEqual, emitUnchangedRow bool) error {
	pt.SetTotal(int64(tbl1.RowsCount + tbl2.RowsCount))
	var current int64
	err := iterateAndMatch(db1, db2, tbl1, tbl2, tblIdx1, tblIdx2, func(pk, row1, row2 []byte, off1, off2 uint32) {
		current++
		pt.SetCurrent(current)
		if row2 != nil {
			if emitUnchangedRow || !colsEqual || !bytes.Equal(row1, row2) {
				diffChan <- &objects.Diff{
					PK:        pk,
					Sum:       row1,
					Offset:    off1,
					OldSum:    row2,
					OldOffset: off2,
				}
			}
		} else {
			diffChan <- &objects.Diff{
				PK:     pk,
				Sum:    row1,
				Offset: off1,
			}
		}
	})
	if err != nil {
		return err
	}
	return iterateAndMatch(db2, db1, tbl2, tbl1, tblIdx2, tblIdx1, func(pk, row1, row2 []byte, off1, off2 uint32) {
		current++
		pt.SetCurrent(current)
		if row2 == nil {
			diffChan <- &objects.Diff{
				PK:        pk,
				OldSum:    row1,
				OldOffset: off1,
			}
		}
	})
}
