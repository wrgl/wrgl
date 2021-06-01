// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/diff"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/progress"
	"github.com/wrgl/core/pkg/slice"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/pkg/widgets"
)

func newDiffCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff COMMIT_OR_FILE [COMMIT_OR_FILE]",
		Short: "Shows diff between 2 commits",
		Example: strings.Join([]string{
			`  # show changes compared to previous commit`,
			`  wrgl diff 1a2ed62`,
			``,
			`  # show changes between branches`,
			`  wrgl diff branch-1 branch-2`,
			``,
			`  # show changes between commits`,
			`  wrgl diff 1a2ed6248c7243cdaaecb98ac12213a7 f1cf51efa2c1e22843b0e083efd89792`,
			``,
			`  # show changes between files`,
			`  wrgl diff file-1.csv file-2.csv --primary-key id,name`,
			``,
			`  # show changes between a file and a branch`,
			`  wrgl diff my-file.csv my-branch`,
		}, "\n"),
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := getRepoDir(cmd)
			var kvStore kv.Store
			var fs kv.FileStore
			if rd.Exist() {
				var err error
				kvStore, err = rd.OpenKVStore()
				if err != nil {
					return err
				}
				defer kvStore.Close()
				fs = rd.OpenFileStore()
			}
			raw, err := cmd.Flags().GetBool("raw")
			if err != nil {
				return err
			}
			pk, err := cmd.Flags().GetStringSlice("primary-key")
			if err != nil {
				return err
			}
			memStore := kv.NewMockStore(false)

			db1, commitHash1, commit1, err := getCommit(cmd, kvStore, memStore, raw, pk, args[0])
			if err != nil {
				return err
			}

			db2, commitHash2, commit2, err := getSecondCommit(cmd, kvStore, memStore, raw, pk, args, commit1)
			if err != nil {
				return err
			}

			ts1, err := table.ReadTable(db1, fs, commit1.Table)
			if err != nil {
				return err
			}
			ts2, err := table.ReadTable(db2, fs, commit2.Table)
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
			diffChan, pt := diff.DiffTables(ts1, ts2, 65*time.Millisecond, errChan, false, false)
			if raw {
				return outputRawDiff(cmd, diffChan)
			}
			return outputDiffToTerminal(cmd, db1, db2, commitHash1, commitHash2, ts1.Columns(), ts2.Columns(), ts1.PrimaryKey(), diffChan, pt)
		},
	}
	cmd.Flags().Bool("raw", false, "show diff in raw binary format.")
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "field names to be used as primary key (only applicable if diff target is a file)")
	return cmd
}

func getSecondCommit(cmd *cobra.Command, db kv.DB, memDB *kv.MockStore, raw bool, pk []string, args []string, commit1 *objects.Commit) (inUsedDB kv.DB, hash string, commit *objects.Commit, err error) {
	if len(args) > 1 {
		return getCommit(cmd, db, memDB, raw, pk, args[1])
	}
	if len(commit.Parents) > 0 {
		return getCommit(cmd, db, memDB, raw, pk, hex.EncodeToString(commit.Parents[0]))
	}
	err = fmt.Errorf("specify the second object to diff against")
	return
}

func createInMemCommit(cmd *cobra.Command, db *kv.MockStore, raw bool, pk []string, file *os.File) (string, *objects.Commit, error) {
	defer file.Close()
	csvReader, columns, primaryKeyIndices, err := ingest.ReadColumns(file, pk)
	if err != nil {
		return "", nil, err
	}
	tb := table.NewBuilder(db, db, columns, primaryKeyIndices, seed, 0)
	out := cmd.OutOrStdout()
	if raw {
		out = io.Discard
	}
	sum, err := ingest.Ingest(seed, 1, csvReader, primaryKeyIndices, tb, out)
	if err != nil {
		return "", nil, err
	}
	commit := &objects.Commit{
		Table: sum,
		Time:  time.Now(),
	}
	_, err = versioning.SaveCommit(db, seed, commit)
	if err != nil {
		return "", nil, err
	}
	return file.Name(), commit, nil
}

var filePattern = regexp.MustCompile(`^.*\..+$`)

func getCommit(cmd *cobra.Command, db kv.DB, memStore *kv.MockStore, raw bool, pk []string, cStr string) (inUsedDB kv.DB, hash string, commit *objects.Commit, err error) {
	inUsedDB = db
	var file *os.File
	_, hashb, commit, err := versioning.InterpretCommitName(db, cStr, false)
	if err != nil {
		file, err = os.Open(cStr)
		if err != nil {
			if filePattern.MatchString(cStr) {
				err = fmt.Errorf("can't find file %s", cStr)
				return
			}
			return
		}
	}
	if memStore != nil && file != nil {
		inUsedDB = memStore
		defer file.Close()
		hash, commit, err = createInMemCommit(cmd, memStore, raw, pk, file)
		return inUsedDB, hash, commit, err
	}
	hash = hex.EncodeToString(hashb)
	return
}

func outputRawDiff(cmd *cobra.Command, diffChan <-chan objects.Diff) error {
	writer := objects.NewDiffWriter(cmd.OutOrStdout())
	for event := range diffChan {
		err := writer.Write(&event)
		if err != nil {
			return err
		}
	}
	return nil
}

func uintSliceEqual(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i, u := range a {
		if u != b[i] {
			return false
		}
	}
	return true
}

func outputDiffToTerminal(
	cmd *cobra.Command,
	db1, db2 kv.DB,
	commitHash1, commitHash2 string,
	cols, oldCols, pk []string,
	diffChan <-chan objects.Diff,
	pt progress.Tracker,
) error {
	var (
		addedRowReader   *table.KeyListRowReader
		removedRowReader *table.KeyListRowReader
		addedTable       *widgets.PreviewTable
		removedTable     *widgets.PreviewTable
		rowChangeReader  *diff.RowChangeReader
		rowChangeTable   *widgets.DiffTable
		pkChanged        bool
		colDiff          *objects.ColDiff
	)

	app := tview.NewApplication()
	titleBar := tview.NewTextView().SetDynamicColors(true)
	fmt.Fprintf(titleBar, "[yellow]%s[white] vs [yellow]%s[white]", commitHash1, commitHash2)

	pBar := widgets.NewProgressBar("Comparing...")

	tabPages := widgets.NewTabPages(app)

	usageBar := tableUsageBar()

	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(titleBar, 1, 1, false).
		AddItem(pBar, 1, 1, false).
		AddItem(tabPages, 0, 1, true).
		AddItem(usageBar, 1, 1, false)
	app.SetRoot(flex, true).
		SetFocus(flex).
		EnableMouse(true).
		SetInputCapture(tabPages.ProcessInput)

	progChan := pt.Chan()
	go pt.Run()

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
			pt.Stop()
			cancel()
			flex.RemoveItem(pBar)
			app.SetFocus(tabPages.LastTab())
			app.Draw()
		}()
	mainLoop:
		for {
			select {
			case e := <-progChan:
				pBar.SetTotal(e.Total)
				pBar.SetCurrent(e.Progress)
			case d, ok := <-diffChan:
				if !ok {
					break mainLoop
				}
				switch d.Type {
				case objects.DTColumnChange:
					colDiff = d.ColDiff
					if !uintSliceEqual(colDiff.BasePK, colDiff.OtherPK[0]) {
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
						unchanged, added, removed := slice.CompareStringSlices(pk, slice.IndicesToValues(colDiff.Names, colDiff.BasePK))
						if len(added) > 0 || len(removed) > 0 {
							tabPages.AddTab(
								"Primary key",
								widgets.CreateColumnsList(unchanged, added, removed),
							)
						}
					}
				case objects.DTRow:
					if d.OldSum == nil {
						if addedRowReader == nil {
							addedRowReader = table.NewKeyListRowReader(db1, [][]byte{d.Sum})
							pkIndices, err := slice.KeyIndices(cols, pk)
							if err != nil {
								panic(err)
							}
							addedTable = widgets.NewPreviewTable(addedRowReader, 1, cols, pkIndices)
							tabPages.AddTab("+1 rows", addedTable)
						} else {
							addedRowReader.Add(d.Sum)
							addedTable.SetRowCount(addedRowReader.NumRows())
							tabPages.SetLabel(addedTable, fmt.Sprintf("+%d rows", addedRowReader.NumRows()))
						}
					} else if d.Sum == nil {
						if removedRowReader == nil {
							removedRowReader = table.NewKeyListRowReader(db2, [][]byte{d.OldSum})
							pkIndices, err := slice.KeyIndices(oldCols, pk)
							if err != nil {
								panic(err)
							}
							removedTable = widgets.NewPreviewTable(removedRowReader, 1, oldCols, pkIndices)
							tabPages.AddTab("-1 rows", removedTable)
						} else {
							removedRowReader.Add(d.OldSum)
							removedTable.SetRowCount(removedRowReader.NumRows())
							tabPages.SetLabel(removedTable, fmt.Sprintf("-%d rows", removedRowReader.NumRows()))
						}
					} else {
						if rowChangeReader == nil {
							var err error
							rowChangeReader, err = diff.NewRowChangeReader(db1, db2, colDiff)
							if err != nil {
								panic(err)
							}
							rowChangeReader.AddRowPair(d.Sum, d.OldSum)
							rowChangeTable = widgets.NewDiffTable(rowChangeReader)
							tabPages.AddTab("1 modified", rowChangeTable)
						} else {
							rowChangeReader.AddRowPair(d.Sum, d.OldSum)
							rowChangeTable.UpdateRowCount()
							tabPages.SetLabel(rowChangeTable, fmt.Sprintf("%d modified", rowChangeReader.NumRows()))
						}
					}
				}
			}
		}

		if !pkChanged && addedRowReader == nil && removedRowReader == nil && rowChangeReader == nil {
			if len(cols) > 0 && !slice.StringSliceEqual(cols, oldCols) {
				if len(cols) == len(oldCols) {
					renamedCols := [][2]string{}
					for i, col := range cols {
						if col != oldCols[i] {
							renamedCols = append(renamedCols, [2]string{oldCols[i], col})
						}
					}
					renamedColumns := tview.NewTextView().
						SetDynamicColors(true)
					for _, names := range renamedCols {
						fmt.Fprintf(renamedColumns, "[red]%s[white] → [green]%s[white]\n", names[0], names[1])
					}
					tabPages.AddTab(
						fmt.Sprintf("%d renamed columns", len(renamedCols)),
						renamedColumns,
					)
				} else {
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
				}
			} else {
				app.Stop()
				cmd.Println("There are no changes!")
			}
		}
	}()
	return app.Run()
}
