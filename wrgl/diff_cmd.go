package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/diff"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/slice"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/pkg/widgets"
)

const (
	diffFormatJSON     = "json"
	diffFormatTerminal = "terminal"
)

func newDiffCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff COMMIT1 COMMIT2",
		Short: "Shows diff between 2 commits",
		Example: strings.Join([]string{
			`  # show diff between branches`,
			`  wrgl diff branch-1 branch-2`,
			``,
			`  # show diff between commits`,
			`  wrgl diff 1a2ed6248c7243cdaaecb98ac12213a7 f1cf51efa2c1e22843b0e083efd89792`,
			``,
			`  # show diff between files`,
			`  wrgl diff file-1.csv file-2.csv`,
		}, "\n"),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cStr1 := args[0]
			cStr2 := args[1]
			format, err := cmd.Flags().GetString("format")
			if err != nil {
				return err
			}
			if format != diffFormatJSON && format != diffFormatTerminal {
				return fmt.Errorf("--format flag is not valid. Valid options are \"json\", \"terminal\"")
			}
			return diffCommits(cmd, cStr1, cStr2, format)
		},
	}
	cmd.Flags().StringP("format", "f", diffFormatTerminal, "output format, valid options are \"json\", \"terminal\" (default)")
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "field names to be used as primary key (only applicable if diff target is a file)")
	return cmd
}

func createInMemCommit(cmd *cobra.Command, db *kv.MockStore, file *os.File) (string, *versioning.Commit, error) {
	defer file.Close()
	pk, err := cmd.Flags().GetStringSlice("primary-key")
	if err != nil {
		return "", nil, err
	}
	csvReader, columns, primaryKeyIndices, err := ingest.ReadColumns(file, pk)
	if err != nil {
		return "", nil, err
	}
	ts := table.NewSmallStore(db, columns, primaryKeyIndices, seed)
	sum, err := ingest.Ingest(seed, 1, csvReader, primaryKeyIndices, ts, cmd.OutOrStdout())
	if err != nil {
		return "", nil, err
	}
	commit := &versioning.Commit{
		ContentHash:    sum,
		Timestamp:      time.Now(),
		TableStoreType: table.Small,
	}
	_, err = commit.Save(db, seed)
	if err != nil {
		return "", nil, err
	}
	return file.Name(), commit, nil
}

func getCommit(cmd *cobra.Command, db kv.Store, memStore *kv.MockStore, cStr string) (inUsedDB kv.Store, hash string, commit *versioning.Commit, err error) {
	inUsedDB = db
	var file *os.File
	hash, commit, file, err = versioning.InterpretCommitName(db, cStr)
	if err != nil {
		return
	}
	if memStore != nil && file != nil {
		inUsedDB = memStore
		defer file.Close()
		hash, commit, err = createInMemCommit(cmd, memStore, file)
		return inUsedDB, hash, commit, err
	}
	return
}

func outputDiffToJSON(cmd *cobra.Command, inflatedChan <-chan diff.InflatedDiff) error {
	out := cmd.OutOrStdout()
	for event := range inflatedChan {
		b, err := json.Marshal(event)
		if err != nil {
			return err
		}
		fmt.Fprintln(out, string(b))
	}
	return nil
}

func outputDiffToTerminal(cmd *cobra.Command, db1, db2 kv.DB, commitHash1, commitHash2 string, diffChan <-chan diff.Diff) error {
	var (
		cols, oldCols, pk []string
		addedRowReader    *table.KeyListRowReader
		removedRowReader  *table.KeyListRowReader
		addedTable        *widgets.PreviewTable
		removedTable      *widgets.PreviewTable
		rowChangeReader   *diff.RowChangeReader
		rowChangeTable    *widgets.DiffTable
		pkChanged         bool
	)

	titleBar := tview.NewTextView().SetDynamicColors(true)
	fmt.Fprintf(titleBar, "[yellow]%s[white] vs [yellow]%s[white]", commitHash1, commitHash2)

	pBar := widgets.NewProgressBar("Comparing...")

	tabPages := widgets.NewTabPages()

	usageBar := tableUsageBar()

	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(titleBar, 1, 1, false).
		AddItem(pBar, 1, 1, false).
		AddItem(tabPages, 0, 1, true).
		AddItem(usageBar, 1, 1, false)
	app := tview.NewApplication().
		SetRoot(flex, true).
		SetFocus(flex).
		EnableMouse(true).
		SetInputCapture(tabPages.ProcessInput)

	// redraw every 65 ms
	drawCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticker := time.NewTicker(65 * time.Millisecond)
	go func() {
		for {
			select {
			case <-ticker.C:
				app.Draw()
			case <-drawCtx.Done():
				return
			}
		}
	}()

	go func() {
		defer func() {
			cancel()
			flex.RemoveItem(pBar)
			app.SetFocus(tabPages.LastTab())
			app.Draw()
		}()
		for event := range diffChan {
			switch event.Type {
			case diff.Init:
				cols, oldCols, pk = event.Columns, event.OldColumns, event.PK
			case diff.Progress:
				pBar.SetTotal(event.Total)
				pBar.SetCurrent(event.Progress)
			case diff.PrimaryKey:
				pkChanged = true
				_, addedCols, removedCols := slice.CompareStringSlices(cols, oldCols)
				if len(addedCols) > 0 {
					tabPages.AddTab(
						fmt.Sprintf("+%d columns", len(addedCols)),
						widgets.CreateColumnsList(nil, addedCols, nil),
					)
				}
				if len(removedCols) > 0 {
					tabPages.AddTab(
						fmt.Sprintf("-%d columns", len(removedCols)),
						widgets.CreateColumnsList(nil, nil, removedCols),
					)
				}
				unchanged, added, removed := slice.CompareStringSlices(pk, event.OldPK)
				if len(added) > 0 || len(removed) > 0 {
					tabPages.AddTab(
						"Primary key",
						widgets.CreateColumnsList(unchanged, added, removed),
					)
				}
			case diff.RowAdd:
				if addedRowReader == nil {
					addedRowReader = table.NewKeyListRowReader(db1, []string{event.Row})
					pkIndices, err := slice.KeyIndices(cols, pk)
					if err != nil {
						panic(err)
					}
					addedTable = widgets.NewPreviewTable(addedRowReader, 1, cols, pkIndices)
					tabPages.AddTab("+1 rows", addedTable)
				} else {
					addedRowReader.Add(event.Row)
					addedTable.SetRowCount(addedRowReader.NumRows())
					tabPages.SetLabel(addedTable, fmt.Sprintf("+%d rows", addedRowReader.NumRows()))
				}
			case diff.RowRemove:
				if removedRowReader == nil {
					removedRowReader = table.NewKeyListRowReader(db2, []string{event.Row})
					pkIndices, err := slice.KeyIndices(oldCols, pk)
					if err != nil {
						panic(err)
					}
					removedTable = widgets.NewPreviewTable(removedRowReader, 1, oldCols, pkIndices)
					tabPages.AddTab("-1 rows", removedTable)
				} else {
					removedRowReader.Add(event.Row)
					removedTable.SetRowCount(removedRowReader.NumRows())
					tabPages.SetLabel(removedTable, fmt.Sprintf("-%d rows", removedRowReader.NumRows()))
				}
			case diff.RowChange:
				if rowChangeReader == nil {
					var err error
					rowChangeReader, err = diff.NewRowChangeReader(db1, db2, cols, oldCols, pk)
					if err != nil {
						panic(err)
					}
					rowChangeReader.AddRowPair(event.Row, event.OldRow)
					rowChangeTable = widgets.NewDiffTable(rowChangeReader)
					tabPages.AddTab("1 modified", rowChangeTable)
				} else {
					rowChangeReader.AddRowPair(event.Row, event.OldRow)
					rowChangeTable.UpdateRowCount()
					tabPages.SetLabel(rowChangeTable, fmt.Sprintf("%d modified", rowChangeReader.NumRows()))
				}
			}
		}

		if !pkChanged && addedRowReader == nil && removedRowReader == nil && rowChangeReader == nil {
			if len(cols) > 0 && !slice.StringSliceEqual(cols, oldCols) {
				renamedCols := [][2]string{}
				for i, col := range cols {
					if col != oldCols[i] {
						renamedCols = append(renamedCols, [2]string{oldCols[i], col})
					}
				}
				if len(renamedCols) > 0 {
					renamedColumns := tview.NewTextView().
						SetDynamicColors(true)
					for _, names := range renamedCols {
						fmt.Fprintf(renamedColumns, "[red]%s[white] → [green]%s[white]\n", names[0], names[1])
					}
					tabPages.AddTab(
						fmt.Sprintf("%d renamed columns", len(renamedCols)),
						renamedColumns,
					)
				}
			} else {
				app.Stop()
				cmd.Println("There are no changes!")
			}
		}
	}()
	return app.Run()
}

func diffCommits(cmd *cobra.Command, cStr1, cStr2, format string) error {
	rd := getRepoDir(cmd)
	kvStore, err := rd.OpenKVStore()
	if err != nil {
		return err
	}
	defer kvStore.Close()
	fs := rd.OpenFileStore()
	memStore := kv.NewMockStore(false)
	db1, commitHash1, commit1, err := getCommit(cmd, kvStore, memStore, cStr1)
	if err != nil {
		return err
	}
	db2, commitHash2, commit2, err := getCommit(cmd, kvStore, memStore, cStr2)
	if err != nil {
		return err
	}
	if commit1.ContentHash == commit2.ContentHash {
		cmd.Println("There are no changes!")
		return nil
	}
	ts1, err := commit1.GetTable(db1, fs, seed)
	if err != nil {
		return err
	}
	ts2, err := commit2.GetTable(db2, fs, seed)
	if err != nil {
		return err
	}
	errChan := make(chan error, 10)
	defer close(errChan)
	go func() {
		for err := range errChan {
			cmd.PrintErrln(err.Error())
			os.Exit(1)
		}
	}()
	diffChan := diff.DiffTables(ts1, ts2, 65*time.Millisecond, errChan)
	if format == diffFormatJSON {
		inflatedChan := diff.Inflate(db1, db2, diffChan, errChan)
		return outputDiffToJSON(cmd, inflatedChan)
	}
	return outputDiffToTerminal(cmd, db1, db2, commitHash1, commitHash2, diffChan)
}
