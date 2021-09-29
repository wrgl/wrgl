// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/cmd/wrgl/utils"
	"github.com/wrgl/core/pkg/diff"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	"github.com/wrgl/core/pkg/progress"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/slice"
	"github.com/wrgl/core/pkg/widgets"
)

func newDiffCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff COMMIT_OR_FILE [COMMIT_OR_FILE]",
		Short: "Shows changes between two commits",
		Long:  "Shows changes between two commits with an interactive diff table. A commit can be specified using shorten sum, full sum, or a reference name. If only one commit is specified, it will be compared with a parent commit. It is also possible to specify a local CSV file instead of a commit, in which case both arguments must be given and the flag --primary-key should also be set.",
		Example: strings.Join([]string{
			`  # show changes compared to the previous commit`,
			`  wrgl diff 1a2ed62`,
			``,
			`  # don't show the interactive table, output to a CSV file instead`,
			`  wrgl diff 1a2ed62 --no-gui`,
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
			`  # show changes between a file and the head commit from a branch`,
			`  wrgl diff my-file.csv my-branch`,
		}, "\n"),
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			debugFile, cleanup, err := setupDebug(cmd)
			if err != nil {
				return err
			}
			defer cleanup()
			rd := utils.GetRepoDir(cmd)
			var db objects.Store
			var rs ref.Store
			if rd.Exist() {
				var err error
				db, err = rd.OpenObjectsStore()
				if err != nil {
					return err
				}
				defer db.Close()
				rs = rd.OpenRefStore()
			}
			noGUI, err := cmd.Flags().GetBool("no-gui")
			if err != nil {
				return err
			}
			pk, err := cmd.Flags().GetStringSlice("primary-key")
			if err != nil {
				return err
			}
			memStore := objmock.NewStore()

			db1, name1, commitHash1, commit1, err := getCommit(cmd, db, memStore, rs, pk, args[0])
			if err != nil {
				return err
			}

			db2, name2, commitHash2, commit2, err := getSecondCommit(cmd, db, memStore, rs, pk, args, commit1)
			if err != nil {
				return err
			}

			tbl1, err := objects.GetTable(db1, commit1.Table)
			if err != nil {
				return err
			}
			tblIdx1, err := objects.GetTableIndex(db1, commit1.Table)
			if err != nil {
				return err
			}
			tbl2, err := objects.GetTable(db2, commit2.Table)
			if err != nil {
				return err
			}
			tblIdx2, err := objects.GetTableIndex(db2, commit2.Table)
			if err != nil {
				return err
			}
			if err != nil {
				return err
			}
			cd := diff.CompareColumns(
				[2][]string{tbl2.Columns, tbl2.PrimaryKey()},
				[2][]string{tbl1.Columns, tbl1.PrimaryKey()},
			)
			errChan := make(chan error, 10)
			opts := []diff.DiffOption{
				diff.WithProgressInterval(65 * time.Millisecond),
			}
			if debugFile != nil {
				opts = append(opts, diff.WithDebugOutput(debugFile))
			}
			diffChan, pt := diff.DiffTables(db1, db2, tbl1, tbl2, tblIdx1, tblIdx2, errChan, opts...)
			if noGUI {
				err = outputDiffToCSV(
					cmd, db1, db2, name1, name2, commitHash1, commitHash2,
					tbl1, tbl2, diffChan, pt, cd,
				)
			} else {
				err = outputDiffToTerminal(
					cmd, db1, db2, name1, name2, commitHash1, commitHash2,
					tbl1, tbl2, diffChan, pt, cd,
				)
			}
			if err != nil {
				return err
			}
			close(errChan)
			err, ok := <-errChan
			if ok {
				return err
			}
			return nil
		},
	}
	cmd.Flags().Bool("no-gui", false, "don't show the diff table, instead output changes to file DIFF_SUM1_SUM2.csv")
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "field names to be used as primary key (only applicable if diff target is a file)")
	return cmd
}

func getSecondCommit(
	cmd *cobra.Command, db objects.Store, memDB *objmock.Store, rs ref.Store, pk []string, args []string, commit1 *objects.Commit,
) (inUsedDB objects.Store, name, hash string, commit *objects.Commit, err error) {
	if len(args) > 1 {
		return getCommit(cmd, db, memDB, rs, pk, args[1])
	}
	if len(commit1.Parents) > 0 {
		return getCommit(cmd, db, memDB, rs, pk, hex.EncodeToString(commit1.Parents[0]))
	}
	err = fmt.Errorf("specify the second object to diff against")
	return
}

func createInMemCommit(cmd *cobra.Command, db *objmock.Store, pk []string, file *os.File) (hash string, commit *objects.Commit, err error) {
	sortPT, blkPT := displayCommitProgress(cmd)
	sum, err := ingest.IngestTable(db, file, pk,
		ingest.WithSortProgressBar(sortPT),
		ingest.WithProgressBar(blkPT),
	)
	if err != nil {
		return
	}
	commit = &objects.Commit{
		Table: sum,
		Time:  time.Now(),
	}
	buf := bytes.NewBuffer(nil)
	_, err = commit.WriteTo(buf)
	if err != nil {
		return
	}
	sum, err = objects.SaveCommit(db, buf.Bytes())
	if err != nil {
		return
	}
	return hex.EncodeToString(sum), commit, nil
}

var filePattern = regexp.MustCompile(`^.*\..+$`)

func displayableCommitName(name string, sum []byte) string {
	if name == hex.EncodeToString(sum) {
		return ""
	}
	name = strings.TrimPrefix(name, "refs/")
	return strings.TrimPrefix(name, "heads/")
}

func getCommit(
	cmd *cobra.Command, db objects.Store, memStore *objmock.Store, rs ref.Store, pk []string, cStr string,
) (inUsedDB objects.Store, name, hash string, commit *objects.Commit, err error) {
	inUsedDB = db
	var file *os.File
	name, hashb, commit, err := ref.InterpretCommitName(db, rs, cStr, false)
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
		hash, commit, err = createInMemCommit(cmd, memStore, pk, file)
		return inUsedDB, path.Base(file.Name()), hash, commit, err
	}
	name = displayableCommitName(cStr, hashb)
	hash = hex.EncodeToString(hashb)
	return
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

func collectDiffObjects(
	cmd *cobra.Command,
	db1, db2 objects.Store,
	tbl1, tbl2 *objects.Table,
	diffChan <-chan *objects.Diff,
	pt progress.Tracker,
	colDiff *diff.ColDiff,
) (addedRowReader, removedRowReader *diff.RowListReader, rowChangeReader *diff.RowChangeReader, err error) {
	progChan := pt.Start()
	defer pt.Stop()
	bar := pbar(0, "Collecting changes", cmd.OutOrStdout(), cmd.ErrOrStderr())
mainLoop:
	for {
		select {
		case e := <-progChan:
			if bar.GetMax() == 0 {
				bar.ChangeMax64(e.Total)
			}
			bar.Set64(e.Progress)
		case d, ok := <-diffChan:
			if !ok {
				break mainLoop
			}
			if d.OldSum == nil {
				if addedRowReader == nil {
					addedRowReader, err = diff.NewRowListReader(db1, tbl1)
					if err != nil {
						return
					}
				}
				addedRowReader.Add(d.Offset)
			} else if d.Sum == nil {
				if removedRowReader == nil {
					removedRowReader, err = diff.NewRowListReader(db2, tbl2)
					if err != nil {
						return
					}
				}
				removedRowReader.Add(d.OldOffset)
			} else {
				if rowChangeReader == nil {
					rowChangeReader, err = diff.NewRowChangeReader(db1, db2, tbl1, tbl2, colDiff)
					if err != nil {
						return
					}
				}
				rowChangeReader.AddRowDiff(d)
			}
		}
	}
	return
}

func rowLabel(label, commitName, commitSum string) string {
	if commitName == "" {
		return fmt.Sprintf("%s (%s)", label, commitSum[:7])
	}
	return fmt.Sprintf("%s %s (%s)", label, commitName, commitSum[:7])
}

func writeRowChanges(
	cmd *cobra.Command,
	w *csv.Writer,
	db1, db2 objects.Store,
	name1, name2 string,
	commitHash1, commitHash2 string,
	tbl1, tbl2 *objects.Table,
	diffChan <-chan *objects.Diff,
	pt progress.Tracker,
	colDiff *diff.ColDiff,
) (err error) {
	buf, err := diff.NewBlockBuffer([]objects.Store{db1, db2}, []*objects.Table{tbl1, tbl2})
	if err != nil {
		return
	}
	progChan := pt.Start()
	defer pt.Stop()
	bar := pbar(0, "Collecting changes", cmd.OutOrStdout(), cmd.ErrOrStderr())
mainLoop:
	for {
		select {
		case e := <-progChan:
			if bar.GetMax() == 0 {
				bar.ChangeMax64(e.Total)
			}
			bar.Set64(e.Progress)
		case d, ok := <-diffChan:
			if !ok {
				break mainLoop
			}
			var row, oldRow []string
			if d.Sum != nil {
				blk, off := diff.RowToBlockAndOffset(d.Offset)
				row, err = buf.GetRow(0, blk, off)
				if err != nil {
					return err
				}
				row = colDiff.RearrangeRow(0, row)
			}
			if d.OldSum != nil {
				blk, off := diff.RowToBlockAndOffset(d.OldOffset)
				oldRow, err = buf.GetRow(1, blk, off)
				if err != nil {
					return err
				}
				oldRow = colDiff.RearrangeBaseRow(oldRow)
			}

			if d.OldSum == nil {
				err = w.Write(append(
					[]string{rowLabel("ADDED IN", name1, commitHash1)},
					row...,
				))
				if err != nil {
					return err
				}
			} else if d.Sum == nil {
				err = w.Write(append(
					[]string{rowLabel("REMOVED IN", name1, commitHash1)},
					oldRow...,
				))
				if err != nil {
					return err
				}
			} else {
				err = w.Write(append(
					[]string{rowLabel("BASE ROW IN", name2, commitHash2)},
					oldRow...,
				))
				if err != nil {
					return err
				}
				err = w.Write(append(
					[]string{rowLabel("MODIFIED IN", name1, commitHash1)},
					row...,
				))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func outputDiffToCSV(
	cmd *cobra.Command,
	db1, db2 objects.Store,
	name1, name2 string,
	commitHash1, commitHash2 string,
	tbl1, tbl2 *objects.Table,
	diffChan <-chan *objects.Diff,
	pt progress.Tracker,
	colDiff *diff.ColDiff,
) (err error) {
	wd, err := os.Getwd()
	if err != nil {
		return
	}
	filename := fmt.Sprintf("DIFF_%s_%s.csv", commitHash1[:7], commitHash2[:7])
	f, err := os.Create(path.Join(wd, filename))
	if err != nil {
		return
	}
	defer f.Close()
	w := csv.NewWriter(f)

	// write column names for old table
	err = w.Write(append(
		[]string{rowLabel("COLUMNS IN", name2, commitHash2)},
		colDiff.RearrangeBaseRow(colDiff.Names)...,
	))
	if err != nil {
		return
	}

	// write column names for new table
	err = w.Write(append(
		[]string{rowLabel("COLUMNS IN", name1, commitHash1)},
		colDiff.RearrangeRow(0, colDiff.Names)...,
	))
	if err != nil {
		return
	}

	// write primary key for old table
	pkRow := make([]string, colDiff.Len()+1)
	pkRow[0] = rowLabel("PRIMARY KEY IN", name2, commitHash2)
	for _, u := range colDiff.BasePK {
		pkRow[u+1] = "true"
	}
	err = w.Write(pkRow)
	if err != nil {
		return
	}

	// write primary key for new table
	pkRow = make([]string, colDiff.Len()+1)
	pkRow[0] = rowLabel("PRIMARY KEY IN", name1, commitHash1)
	for _, u := range colDiff.OtherPK[0] {
		pkRow[u+1] = "true"
	}
	err = w.Write(pkRow)
	if err != nil {
		return
	}

	if uintSliceEqual(colDiff.BasePK, colDiff.OtherPK[0]) {
		// primary key stays the same, we can compare individual rows now
		err = writeRowChanges(cmd, w, db1, db2, name1, name2, commitHash1, commitHash2, tbl1, tbl2, diffChan, pt, colDiff)
		if err != nil {
			return
		}
	}

	w.Flush()
	cmd.Printf("saved conflicts to file %s\n", filename)
	return nil
}

func commitTitle(commitName, commitSum string) string {
	if commitName == "" {
		return fmt.Sprintf("[yellow]%s[white]", commitSum[:7])
	}
	return fmt.Sprintf("%s ([yellow]%s[white])", commitName, commitSum[:7])
}

func outputDiffToTerminal(
	cmd *cobra.Command,
	db1, db2 objects.Store,
	name1, name2 string,
	commitHash1, commitHash2 string,
	tbl1, tbl2 *objects.Table,
	diffChan <-chan *objects.Diff,
	pt progress.Tracker,
	colDiff *diff.ColDiff,
) (err error) {
	app := tview.NewApplication()
	titleBar := tview.NewTextView().SetDynamicColors(true)
	fmt.Fprintf(titleBar, "%s vs %s", commitTitle(name1, commitHash1), commitTitle(name2, commitHash2))

	tabPages := widgets.NewTabPages(app)

	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(titleBar, 1, 1, false).
		AddItem(tabPages, 0, 1, true)
	app.SetRoot(flex, true).
		SetFocus(flex).
		EnableMouse(true).
		SetInputCapture(tabPages.ProcessInput)

	if !uintSliceEqual(colDiff.BasePK, colDiff.OtherPK[0]) {
		// primary key has changed, we can only show column changes at this point
		_, addedCols, removedCols := slice.CompareStringSlices(tbl1.Columns, tbl2.Columns)
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
		unchanged, added, removed := slice.CompareStringSlices(tbl1.PrimaryKey(), tbl2.PrimaryKey())
		if len(added) > 0 || len(removed) > 0 {
			tabPages.AddTab(
				"Primary key",
				widgets.CreateColumnsList(unchanged, added, removed),
			)
		}
		return app.Run()
	}

	addedRowReader, removedRowReader, rowChangeReader, err := collectDiffObjects(cmd, db1, db2, tbl1, tbl2, diffChan, pt, colDiff)
	if err != nil {
		return
	}

	if addedRowReader != nil {
		pkIndices, err := slice.KeyIndices(tbl1.Columns, tbl1.PrimaryKey())
		if err != nil {
			return err
		}
		addedTable := widgets.NewPreviewTable(addedRowReader, addedRowReader.Len(), tbl1.Columns, pkIndices)
		tabPages.AddTab(fmt.Sprintf("+%d rows", addedRowReader.Len()), addedTable)
	}
	if removedRowReader != nil {
		pkIndices, err := slice.KeyIndices(tbl2.Columns, tbl1.PrimaryKey())
		if err != nil {
			return err
		}
		removedTable := widgets.NewPreviewTable(removedRowReader, removedRowReader.Len(), tbl2.Columns, pkIndices)
		tabPages.AddTab(fmt.Sprintf("-%d rows", removedRowReader.Len()), removedTable)
	}
	if rowChangeReader != nil {
		rowChangeTable := widgets.NewDiffTable(rowChangeReader)
		tabPages.AddTab(fmt.Sprintf("%d modified", rowChangeReader.NumRows()), rowChangeTable)
	}

	if addedRowReader == nil && removedRowReader == nil && rowChangeReader == nil {
		cmd.Println("There are no changes!")
		return nil
	}

	usageBar := widgets.DataTableUsage()
	flex.AddItem(usageBar, 1, 1, false)
	app.SetBeforeDrawFunc(func(screen tcell.Screen) bool {
		usageBar.BeforeDraw(screen, flex)
		return false
	}).SetFocus(tabPages.LastTab())

	cancel := redrawEvery(app, 65*time.Millisecond)
	defer cancel()
	return app.Run()
}
