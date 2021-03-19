package diff

import (
	"bytes"
	"encoding/hex"
	"io"
	"time"

	"github.com/wrgl/core/pkg/table"
)

type DiffEventType int

const (
	Unspecified DiffEventType = iota
	Init
	Progress
	PrimaryKey
	RowChange
	RowAdd
	RowRemove
)

type DiffEvent struct {
	Type DiffEventType `json:"t"`

	// Init event fields
	OldColumns []string `json:"oldCols,omitempty"`
	Columns    []string `json:"cols,omitempty"`

	// Progress event fields
	Progress int64 `json:"progress,omitempty"`
	Total    int64 `json:"total,omitempty"`

	// PrimaryKey event fields
	OldPK []string `json:"oldPK,omitempty"`
	PK    []string `json:"pk,omitempty"`

	// RowChange event fields
	OldRow string `json:"oldRow,omitempty"`
	// RowAdd, RowRemove event fields
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

func DiffTables(t1, t2 table.Store, diffChan chan<- DiffEvent, progressPeriod time.Duration) error {
	diffChan <- DiffEvent{
		Type:       Init,
		Columns:    t1.Columns(),
		OldColumns: t2.Columns(),
	}

	sl1 := t1.PrimaryKey()
	sl2 := t2.PrimaryKey()
	if len(sl1) == 0 && len(sl2) == 0 && !strSliceEqual(t1.Columns(), t2.Columns()) {
		return nil
	}
	if !strSliceEqual(sl1, sl2) {
		diffChan <- DiffEvent{
			Type:  PrimaryKey,
			PK:    sl1,
			OldPK: sl2,
		}
	} else {
		err := diffRows(t1, t2, diffChan, progressPeriod)
		if err != nil {
			return err
		}
	}
	return nil
}

func diffRows(t1, t2 table.Store, diffChan chan<- DiffEvent, progressPeriod time.Duration) error {
	l1, err := t1.NumRows()
	if err != nil {
		return err
	}
	l2, err := t2.NumRows()
	if err != nil {
		return err
	}
	total := int64(l1 + l2)
	var currentProgress int64
	ticker := time.NewTicker(progressPeriod)
	defer ticker.Stop()
	r1, err := t1.NewRowHashReader(0, 0)
	if err != nil {
		return err
	}
	defer r1.Close()
loop1:
	for {
		select {
		case <-ticker.C:
			diffChan <- DiffEvent{
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
					diffChan <- DiffEvent{
						Type:   RowChange,
						OldRow: hex.EncodeToString(w),
						Row:    hex.EncodeToString(v),
					}
				}
			} else {
				diffChan <- DiffEvent{
					Type: RowAdd,
					Row:  hex.EncodeToString(v),
				}
			}
		}
	}
	r2, err := t2.NewRowHashReader(0, 0)
	if err != nil {
		return err
	}
	defer r2.Close()
loop2:
	for {
		select {
		case <-ticker.C:
			diffChan <- DiffEvent{
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
				diffChan <- DiffEvent{
					Type: RowRemove,
					Row:  hex.EncodeToString(v),
				}
			}
		}
	}
	return nil
}
