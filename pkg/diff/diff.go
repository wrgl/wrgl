// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diff

import (
	"bytes"
	"io"
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

type Differ struct {
	progressInterval time.Duration
	emitUnchangedRow bool
	debugOut         io.Writer
	db1, db2         objects.Store
	tbl1, tbl2       *objects.Table
	tblIdx1, tblIdx2 [][]string
	errChan          chan<- error
}

type DiffOption func(*Differ)

// WithProgressInterval tells DiffTables to emit progress event at specified interval
func WithProgressInterval(du time.Duration) DiffOption {
	return func(d *Differ) {
		d.progressInterval = du
	}
}

// WithEmitUnchangedRow tells DiffTables to emit even row that stayed the same between
// two tables.
func WithEmitUnchangedRow() DiffOption {
	return func(d *Differ) {
		d.emitUnchangedRow = true
	}
}

// WithDebugOutput tells DiffTables to write debug info into the provided writer
func WithDebugOutput(r io.Writer) DiffOption {
	return func(d *Differ) {
		d.debugOut = r
	}
}

func (d *Differ) diffTables() (<-chan *objects.Diff, progress.Tracker) {
	diffChan := make(chan *objects.Diff)
	pt := progress.NewSingleTracker(d.progressInterval, 0)
	go func() {
		defer close(diffChan)

		pkEqual := strSliceEqual(d.tbl1.PrimaryKey(), d.tbl2.PrimaryKey())
		colsEqual := strSliceEqual(d.tbl1.Columns, d.tbl2.Columns)

		if pkEqual && (len(d.tbl1.PK) > 0 || colsEqual) {
			err := d.diffRows(diffChan, pt, colsEqual)
			if err != nil {
				d.errChan <- err
				return
			}
		}
	}()
	return diffChan, pt
}

func DiffTables(db1, db2 objects.Store, tbl1, tbl2 *objects.Table, tblIdx1, tblIdx2 [][]string, errChan chan<- error, opts ...DiffOption) (<-chan *objects.Diff, progress.Tracker) {
	d := &Differ{
		db1:     db1,
		db2:     db2,
		tbl1:    tbl1,
		tbl2:    tbl2,
		tblIdx1: tblIdx1,
		tblIdx2: tblIdx2,
		errChan: errChan,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d.diffTables()
}

func (d *Differ) diffRows(diffChan chan<- *objects.Diff, pt *progress.SingleTracker, colsEqual bool) error {
	pt.SetTotal(int64(d.tbl1.RowsCount + d.tbl2.RowsCount))
	var current int64
	err := iterateAndMatch(d.db1, d.db2, d.tbl1, d.tbl2, d.tblIdx1, d.tblIdx2, d.debugOut, func(pk, row1, row2 []byte, off1, off2 uint32) {
		current++
		pt.SetCurrent(current)
		if row2 != nil {
			// TODO: build a way to debug diff process
			if d.emitUnchangedRow || !colsEqual || !bytes.Equal(row1, row2) {
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
	return iterateAndMatch(d.db2, d.db1, d.tbl2, d.tbl1, d.tblIdx2, d.tblIdx1, d.debugOut, func(pk, row1, row2 []byte, off1, off2 uint32) {
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
