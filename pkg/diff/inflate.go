package diff

import (
	"encoding/hex"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
)

type InflatedDiff struct {
	Type DiffType `json:"t"`

	// Init fields
	OldColumns []string `json:"oldCols,omitempty"`
	Columns    []string `json:"cols,omitempty"`

	// Progress fields
	Progress int64 `json:"progress,omitempty"`
	Total    int64 `json:"total,omitempty"`

	// PrimaryKey fields
	OldPK []string `json:"oldPK,omitempty"`
	PK    []string `json:"pk,omitempty"`

	// RowAdd and RowRemove fields
	Row []string `json:"row,omitempty"`

	// RowChangeInit fields
	RowChangeColumns []*RowChangeColumn `json:"rowChangeColumns,omitempty"`
	RowChangeRow     [][]string         `json:"rowChangeRow,omitempty"`
}

func fetchRow(db kv.DB, row string, dec *objects.StrListDecoder) ([]string, error) {
	k, err := hex.DecodeString(row)
	if err != nil {
		return nil, err
	}
	b, err := table.GetRow(db, k)
	if err != nil {
		return nil, err
	}
	return dec.Decode(b), nil
}

func Inflate(db1, db2 kv.DB, diffChan <-chan Diff, errChan chan error) <-chan InflatedDiff {
	ch := make(chan InflatedDiff)
	go func() {
		var (
			cols, oldCols, pk []string
			rowChangeReader   *RowChangeReader
			dec               = objects.NewStrListDecoder(false)
		)
		defer close(ch)
		for event := range diffChan {
			switch event.Type {
			case Init:
				cols = event.Columns
				oldCols = event.OldColumns
				pk = event.PK
				ch <- InflatedDiff{
					Type:       Init,
					Columns:    event.Columns,
					OldColumns: event.OldColumns,
					PK:         event.PK,
				}
			case Progress:
				ch <- InflatedDiff{
					Type:     Progress,
					Progress: event.Progress,
					Total:    event.Total,
				}
			case PrimaryKey:
				ch <- InflatedDiff{
					Type:  PrimaryKey,
					OldPK: event.OldPK,
				}
			case RowAdd:
				row, err := fetchRow(db1, event.Row, dec)
				if err != nil {
					errChan <- err
					return
				}
				ch <- InflatedDiff{
					Type: event.Type,
					Row:  row,
				}
			case RowRemove:
				row, err := fetchRow(db2, event.Row, dec)
				if err != nil {
					errChan <- err
					return
				}
				ch <- InflatedDiff{
					Type: event.Type,
					Row:  row,
				}
			case RowChange:
				if rowChangeReader == nil {
					var err error
					rowChangeReader, err = NewRowChangeReader(db1, db2, cols, oldCols, pk)
					if err != nil {
						errChan <- err
						return
					}
					ch <- InflatedDiff{
						Type:             RowChangeInit,
						RowChangeColumns: rowChangeReader.Columns,
					}
				}
				rowChangeReader.AddRowPair(event.Row, event.OldRow)
				mergedRow, err := rowChangeReader.Read()
				if err != nil {
					errChan <- err
					return
				}
				ch <- InflatedDiff{
					Type:         RowChange,
					RowChangeRow: mergedRow,
				}
			}
		}
	}()
	return ch
}
