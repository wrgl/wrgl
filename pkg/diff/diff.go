package diff

import (
	"bytes"
	"encoding/hex"
	"io"
	"time"

	"github.com/wrgl/core/pkg/table"
)

type DiffType int

const (
	Unspecified DiffType = iota

	// Diff.Type and InflatedDiff.Type can be set to the following values
	Init
	Progress
	PrimaryKey
	RowChange
	RowAdd
	RowRemove

	// InflatedDiff.Type can be set to this value in addition
	RowChangeInit
	ColumnsRename
)

type Diff struct {
	Type DiffType `json:"t"`

	// Init fields
	OldColumns []string `json:"oldCols,omitempty"`
	Columns    []string `json:"cols,omitempty"`
	PK         []string `json:"pk,omitempty"`

	// Progress fields
	Progress int64 `json:"progress,omitempty"`
	Total    int64 `json:"total,omitempty"`

	// PrimaryKey fields
	OldPK []string `json:"oldPK,omitempty"`

	// RowChange fields
	OldRow string `json:"oldRow,omitempty"`
	// RowAdd, RowRemove fields
	Row string `json:"row,omitempty"`
}

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

func DiffTables(t1, t2 table.Store, progressPeriod time.Duration, errChan chan<- error) <-chan Diff {
	diffChan := make(chan Diff)
	go func() {
		defer close(diffChan)
		diffChan <- Diff{
			Type:       Init,
			Columns:    t1.Columns(),
			OldColumns: t2.Columns(),
			PK:         t1.PrimaryKey(),
		}

		sl1 := t1.PrimaryKey()
		sl2 := t2.PrimaryKey()
		if len(sl1) == 0 && len(sl2) == 0 && !strSliceEqual(t1.Columns(), t2.Columns()) {
			return
		}
		if !strSliceEqual(sl1, sl2) {
			diffChan <- Diff{
				Type:  PrimaryKey,
				OldPK: sl2,
			}
		} else {
			err := diffRows(t1, t2, diffChan, progressPeriod)
			if err != nil {
				errChan <- err
				return
			}
		}
	}()
	return diffChan
}

func diffRows(t1, t2 table.Store, diffChan chan<- Diff, progressPeriod time.Duration) error {
	l1 := t1.NumRows()
	l2 := t2.NumRows()
	total := int64(l1 + l2)
	var currentProgress int64
	ticker := time.NewTicker(progressPeriod)
	defer ticker.Stop()
	r1 := t1.NewRowHashReader(0, 0)
loop1:
	for {
		select {
		case <-ticker.C:
			diffChan <- Diff{
				Type:     Progress,
				Progress: currentProgress,
				Total:    total,
			}
		default:
			k, v, err := r1.Read()
			if err == io.EOF {
				break loop1
			}
			if err != nil {
				return err
			}
			currentProgress++
			if w, ok := t2.GetRowHash(k); ok {
				if !bytes.Equal(v, w) {
					diffChan <- Diff{
						Type:   RowChange,
						OldRow: hex.EncodeToString(w),
						Row:    hex.EncodeToString(v),
					}
				}
			} else {
				diffChan <- Diff{
					Type: RowAdd,
					Row:  hex.EncodeToString(v),
				}
			}
		}
	}
	r2 := t2.NewRowHashReader(0, 0)
loop2:
	for {
		select {
		case <-ticker.C:
			diffChan <- Diff{
				Type:     Progress,
				Progress: currentProgress,
				Total:    total,
			}
		default:
			k, v, err := r2.Read()
			if err == io.EOF {
				break loop2
			}
			if err != nil {
				return err
			}
			currentProgress++
			if _, ok := t1.GetRowHash(k); !ok {
				diffChan <- Diff{
					Type: RowRemove,
					Row:  hex.EncodeToString(v),
				}
			}
		}
	}
	return nil
}
