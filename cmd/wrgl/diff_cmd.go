// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/mitchellh/colorstring"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/diff"
	diffprof "github.com/wrgl/wrgl/pkg/diff/prof"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/pbar"
	"github.com/wrgl/wrgl/pkg/progress"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/slice"
	"github.com/wrgl/wrgl/pkg/sorter"
	"github.com/wrgl/wrgl/pkg/transaction"
	"github.com/wrgl/wrgl/pkg/widgets"
	widgetsprof "github.com/wrgl/wrgl/pkg/widgets/prof"
)

func newDiffCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff { COMMIT | COMMIT_OR_FILE COMMIT_OR_FILE | BRANCH --branch-file | --all | --txid TRANSACTION_ID }",
		Short: "Show changes between two commits",
		Long:  "Show changes between two commits with an interactive diff table. A commit can be specified using shorten sum, full sum, or a reference name. If only one commit is specified, it will be compared with a parent commit. It is also possible to specify a local CSV file instead of a commit, in which case both arguments must be given and the flag --primary-key should also be set.",
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
			``,
			`  # show diff between branch.file config (set with wrgl commit --set-file) and the latest commit of a branch`,
			`  wrgl diff my-branch --branch-file`,
			``,
			`  # show diff summary for branches that have branch.file configured`,
			`  wrgl diff --all`,
			``,
			`  # show diff summary for all changes made with a transaction (run 'wrgl transaction -h' to learn more about transaction)`,
			`  wrgl diff --txid a1dbfcc4-f6da-454c-a783-f1b70d347baf`,
		}, "\n"),
		Args: cobra.RangeArgs(0, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, cleanup, err := utils.SetupDebug(cmd)
			if err != nil {
				return err
			}
			defer cleanup()
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
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
			branchFile, err := cmd.Flags().GetBool("branch-file")
			if err != nil {
				return err
			}
			all, err := cmd.Flags().GetBool("all")
			if err != nil {
				return err
			}
			tid, err := parseTxidFlag(cmd)
			if err != nil {
				return err
			}
			if len(args) == 0 && !all && tid == nil {
				all = true
			}
			memStore := objmock.NewStore()
			s := conffs.NewStore(rd.FullPath, conffs.AggregateSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}

			if tid != nil {
				return diffTransaction(cmd, c, db, rs, logger, *tid)
			}

			if all {
				return diffAllBranches(cmd, c, db, rs, pk, args, logger)
			}

			if noGUI {
				return runDiff(cmd, c, db, memStore, rs, pk, args, branchFile, logger, false, outputDiffToCSV)
			}
			return runDiff(cmd, c, db, memStore, rs, pk, args, branchFile, logger, false, outputDiffToTerminal)
		},
	}
	cmd.Flags().Bool("no-gui", false, "don't show the diff table, instead output changes to file DIFF_SUM1_SUM2.csv")
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "field names to be used as primary key (only applicable if diff target is a file)")
	cmd.Flags().Bool("branch-file", false, "if only one argument is given and it is a branch name, compare against branch.file (if it is configured with wrgl commit --set-file)")
	cmd.Flags().Bool("all", false, "show diff summary for all branches that have branch.file configured. This flag is automatically set when no argument is given and --txid is not set")
	cmd.Flags().String("txid", "", "show diff summary for all changes with specified transaction id")
	cmd.Flags().String("delimiter-1", "", "CSV delimiter of the first argument if the first argument is an external file. Defaults to comma.")
	cmd.Flags().String("delimiter-2", "", "CSV delimiter of the second argument if the second argument is an external file. Defaults to comma.")
	return cmd
}

func getSecondCommit(
	cmd *cobra.Command, c *conf.Config, db objects.Store, memDB *objmock.Store, rs ref.Store,
	pk []string, args []string, commit1 *objects.Commit, branchFile, quiet bool, delim rune,
) (inUsedDB objects.Store, name, hash string, commit *objects.Commit, err error) {
	if branchFile {
		return getCommit(cmd, c, db, memDB, rs, pk, args[0], false, quiet, delim)
	}
	if len(args) > 1 {
		return getCommit(cmd, c, db, memDB, rs, pk, args[1], false, quiet, delim)
	}
	if len(commit1.Parents) > 0 {
		return getCommit(cmd, c, db, memDB, rs, pk, hex.EncodeToString(commit1.Parents[0]), false, quiet, delim)
	}
	err = fmt.Errorf("specify the second object to diff against")
	return
}

func createInMemCommit(cmd *cobra.Command, db *objmock.Store, pk []string, file *os.File, quiet bool, delim rune) (hash []byte, commit *objects.Commit, err error) {
	sum, err := ingestTable(
		cmd, db, file, pk, quiet,
		[]sorter.SorterOption{
			sorter.WithDelimiter(delim),
		},
		[]ingest.InserterOption{},
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
	commit.Sum = sum
	return sum, commit, nil
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
	cmd *cobra.Command, c *conf.Config, db objects.Store, memStore *objmock.Store,
	rs ref.Store, pk []string, cStr string, branchFile, quiet bool, delim rune,
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
		inUsedDB = memStore
		defer file.Close()
		hashb, commit, err = createInMemCommit(cmd, memStore, pk, file, quiet, delim)
		hash = hex.EncodeToString(hashb)
		return inUsedDB, path.Base(file.Name()), hash, commit, err
	}
	if branchFile && strings.HasPrefix(name, "heads/") {
		branchName := strings.TrimPrefix(name, "heads/")
		errFileNotSet := fmt.Errorf("illegal flag --branch-file: branch.file is not set for branch %q", branchName)
		if c.Branch == nil {
			err = errFileNotSet
			return
		}
		if branch, ok := c.Branch[branchName]; !ok {
			err = errFileNotSet
			return
		} else if branch.File == "" {
			err = errFileNotSet
			return
		} else {
			var tmpSum []byte
			tmpSum, err = ensureTempCommit(cmd, db, rs, c, branchName, branch.File, branch.PrimaryKey, 1, 0, quiet, delim)
			if err != nil {
				return
			}
			commit, err = objects.GetCommit(db, tmpSum)
			if err != nil {
				return
			}
			name = filepath.Base(branch.File)
			hash = hex.EncodeToString(tmpSum)
			return
		}
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
	quiet bool,
) (addedRowReader, removedRowReader *diff.RowListReader, rowChangeReader *diff.RowChangeReader, err error) {
	var progChan <-chan progress.Event
	if pt != nil {
		progChan = pt.Start()
		defer pt.Stop()
	}
	if err = utils.WithProgressBar(cmd, quiet, func(cmd *cobra.Command, barContainer pbar.Container) (err error) {
		bar := barContainer.NewBar(-1, "Collecting changes", 0)
		defer bar.Done()
	mainLoop:
		for {
			select {
			case e := <-progChan:
				bar.SetTotal(e.Total)
				bar.SetCurrent(e.Progress)
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
		return nil
	}); err != nil {
		return
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
	if err = utils.WithProgressBar(cmd, false, func(cmd *cobra.Command, barContainer pbar.Container) (err error) {
		bar := barContainer.NewBar(-1, "Collecting changes", 0)
		defer bar.Done()
	mainLoop:
		for {
			select {
			case e := <-progChan:
				bar.SetTotal(e.Total)
				bar.SetCurrent(e.Progress)
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
						[]string{rowLabel("BASE ROW FROM", name2, commitHash2)},
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
	}); err != nil {
		return err
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
	tpd *diffprof.TableProfileDiff,
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
	cmd.Printf("saved changes to file %s\n", filename)
	return nil
}

func commitTitle(commitName, commitSum string) string {
	if commitName == "" {
		return fmt.Sprintf("[yellow]%s[white]", commitSum[:7])
	}
	return fmt.Sprintf("%s ([yellow]%s[white])", commitName, commitSum[:7])
}

func getDiffChan(
	db1, db2 objects.Store, rs ref.Store, commit1, commit2 *objects.Commit, logger *logr.Logger,
) (tbl1, tbl2 *objects.Table, diffChan <-chan *objects.Diff, pt progress.Tracker, cd *diff.ColDiff, errChan chan error, err error) {
	tbl1, err = utils.GetTable(db1, rs, commit1)
	if err != nil {
		return
	}
	tblIdx1, err := objects.GetTableIndex(db1, commit1.Table)
	if err != nil {
		return
	}
	tbl2, err = utils.GetTable(db2, rs, commit2)
	if err != nil {
		return
	}
	tblIdx2, err := objects.GetTableIndex(db2, commit2.Table)
	if err != nil {
		return
	}
	if err != nil {
		return
	}
	cd = diff.CompareColumns(
		[2][]string{tbl2.Columns, tbl2.PrimaryKey()},
		[2][]string{tbl1.Columns, tbl1.PrimaryKey()},
	)
	errChan = make(chan error, 10)
	opts := []diff.DiffOption{}
	opts = append(opts, diff.WithProgressInterval(65*time.Millisecond))
	if logger != nil {
		opts = append(opts, diff.WithDebugLogger(logger))
	}
	diffChan, pt = diff.DiffTables(db1, db2, tbl1, tbl2, tblIdx1, tblIdx2, errChan, opts...)
	return tbl1, tbl2, diffChan, pt, cd, errChan, nil
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
	tpd *diffprof.TableProfileDiff,
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

	addedRowReader, removedRowReader, rowChangeReader, err := collectDiffObjects(cmd, db1, db2, tbl1, tbl2, diffChan, pt, colDiff, false)
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
		tabPages.AddTab(fmt.Sprintf("%d modified", rowChangeReader.Len()), rowChangeTable)
	}
	if tpd != nil {
		profileTable, err := widgetsprof.NewStatDiffTable(tpd)
		if err != nil {
			return err
		}
		tabPages.AddTab("profile", profileTable)
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

func outputDiffSummaryToTerminal(
	cmd *cobra.Command,
	db1, db2 objects.Store,
	name1, name2 string,
	commitHash1, commitHash2 string,
	tbl1, tbl2 *objects.Table,
	diffChan <-chan *objects.Diff,
	cd *diff.ColDiff,
) (string, error) {
	sb := &strings.Builder{}
	if !uintSliceEqual(cd.BasePK, cd.OtherPK[0]) {
		_, addedCols, removedCols := slice.CompareStringSlices(tbl1.Columns, tbl2.Columns)
		pkEqual := slice.StringSliceEqual(tbl1.PrimaryKey(), tbl2.PrimaryKey())
		if len(addedCols) > 0 || len(removedCols) > 0 {
			sb.WriteString("columns: ")
			if len(addedCols) > 0 {
				colorstring.Fprintf(sb, "[green]+%d[reset]", len(addedCols))
				if len(removedCols) > 0 {
					sb.WriteString("/")
				}
			}
			if len(removedCols) > 0 {
				colorstring.Fprintf(sb, "[red]-%d[reset]", len(removedCols))
			}
			if !pkEqual {
				sb.WriteString("; ")
			}
		}
		if !pkEqual {
			colorstring.Fprintf(sb,
				"primary key: [red]%s[reset]->[green]%s",
				strings.Join(tbl2.PrimaryKey(), ","),
				strings.Join(tbl1.PrimaryKey(), ","),
			)
		}
	} else {
		addedRowReader, removedRowReader, rowChangeReader, err := collectDiffObjects(cmd, db1, db2, tbl1, tbl2, diffChan, nil, cd, true)
		if err != nil {
			return "", err
		}
		nAdd, nRem, nMod := addedRowReader.Len(), removedRowReader.Len(), rowChangeReader.Len()
		if nAdd > 0 || nRem > 0 || nMod > 0 {
			sb.WriteString("rows: ")
			outputs := []string{}
			if nAdd > 0 {
				outputs = append(outputs, fmt.Sprintf("[green]+%d[reset]", nAdd))
			}
			if nRem > 0 {
				outputs = append(outputs, fmt.Sprintf("[red]-%d[reset]", nRem))
			}
			if nMod > 0 {
				outputs = append(outputs, fmt.Sprintf("[yellow]m%d[reset]", nMod))
			}
			colorstring.Fprintf(sb, strings.Join(outputs, "/"))
		}
	}
	return sb.String(), nil
}

func diffTableProfiles(db1, db2 objects.Store, commit1, commit2 *objects.Commit) *diffprof.TableProfileDiff {
	prof1, err := objects.GetTableProfile(db1, commit1.Table)
	if err != nil {
		return nil
	}
	prof2, err := objects.GetTableProfile(db2, commit2.Table)
	if err != nil {
		return nil
	}
	return diffprof.DiffTableProfiles(prof1, prof2)
}

func runDiff(
	cmd *cobra.Command, c *conf.Config, db objects.Store, memStore *objmock.Store, rs ref.Store,
	pk []string, args []string, branchFile bool, logger *logr.Logger, quiet bool,
	outputDiff func(
		cmd *cobra.Command,
		db1, db2 objects.Store,
		name1, name2 string,
		commitHash1, commitHash2 string,
		tbl1, tbl2 *objects.Table,
		diffChan <-chan *objects.Diff,
		pt progress.Tracker,
		colDiff *diff.ColDiff,
		tpd *diffprof.TableProfileDiff,
	) error,
) error {
	delim1, err := utils.GetRuneFromFlag(cmd, "delimiter-1")
	if err != nil {
		return err
	}
	db1, name1, commitHash1, commit1, err := getCommit(cmd, c, db, memStore, rs, pk, args[0], branchFile, quiet, delim1)
	if err != nil {
		return err
	}

	delim2, err := utils.GetRuneFromFlag(cmd, "delimiter-2")
	if err != nil {
		return err
	}
	db2, name2, commitHash2, commit2, err := getSecondCommit(cmd, c, db, memStore, rs, pk, args, commit1, branchFile, quiet, delim2)
	if err != nil {
		return err
	}

	tbl1, tbl2, diffChan, pt, cd, errChan, err := getDiffChan(db1, db2, rs, commit1, commit2, logger)
	if err != nil {
		return err
	}

	tpd := diffTableProfiles(db1, db2, commit1, commit2)
	if err = outputDiff(
		cmd, db1, db2, name1, name2, commitHash1, commitHash2,
		tbl1, tbl2, diffChan, pt, cd, tpd,
	); err != nil {
		return err
	}

	close(errChan)
	err, ok := <-errChan
	if ok {
		return err
	}
	return nil
}

type diffArgs struct {
	Branch  string
	PK      []string
	Commits []string
}

func diffMultiple(cmd *cobra.Command, c *conf.Config, db objects.Store, rs ref.Store, logger *logr.Logger, dargs []diffArgs, branchFile, quiet bool) (err error) {
	var maxLen int
	for _, darg := range dargs {
		if len(darg.Branch) > maxLen {
			maxLen = len(darg.Branch)
		}
	}
	sort.Slice(dargs, func(i, j int) bool {
		return dargs[i].Branch < dargs[j].Branch
	})
	for _, darg := range dargs {
		var diffSum string
		if err := runDiff(cmd, c, db, nil, rs, darg.PK, darg.Commits, branchFile, logger, quiet,
			func(
				cmd *cobra.Command, db1, db2 objects.Store, name1, name2, commitHash1, commitHash2 string,
				tbl1, tbl2 *objects.Table, diffChan <-chan *objects.Diff, pt progress.Tracker,
				cd *diff.ColDiff, tpd *diffprof.TableProfileDiff,
			) (err error) {
				diffSum, err = outputDiffSummaryToTerminal(
					cmd, db1, db2, name1, name2, commitHash1, commitHash2,
					tbl1, tbl2, diffChan, cd,
				)
				return
			},
		); err != nil {
			return err
		}
		if diffSum != "" {
			n := len(darg.Branch)
			padding := strings.Repeat(" ", maxLen-n)
			colorstring.Fprintf(cmd.OutOrStdout(), "[bold]%s[reset]%s %s\n", darg.Branch, padding, diffSum)
		}
	}
	return nil
}

func diffTransaction(cmd *cobra.Command, c *conf.Config, db objects.Store, rs ref.Store, logger *logr.Logger, tid uuid.UUID) (err error) {
	m, _, err := transaction.Diff(rs, tid)
	if err != nil {
		return
	}
	dargs := []diffArgs{}
	cmd.Printf("Changes from transaction %s\n", tid.String())
	for name, sums := range m {
		if sums[1] == nil {
			cmd.Printf("Branch %q didn't previously exist, skipping.\n", name)
			continue
		}
		dargs = append(dargs, diffArgs{
			Branch:  name,
			Commits: []string{hex.EncodeToString(sums[0]), hex.EncodeToString(sums[1])},
		})
	}
	return diffMultiple(cmd, c, db, rs, logger, dargs, false, true)
}

func diffAllBranches(
	cmd *cobra.Command, c *conf.Config, db objects.Store, rs ref.Store,
	pk []string, args []string, logger *logr.Logger,
) error {
	dargs := []diffArgs{}
	for name, branch := range c.Branch {
		if branch.File == "" {
			continue
		}
		if _, err := os.Stat(branch.File); os.IsNotExist(err) {
			cmd.Printf("File %q does not exist, skipping branch %q.\n", branch.File, name)
			continue
		}
		if _, err := ref.GetHead(rs, name); err == ref.ErrKeyNotFound {
			cmd.Printf("Branch %q not found, skipping.\n", name)
			continue
		} else if err != nil {
			return err
		}
		dargs = append(dargs, diffArgs{
			Branch:  name,
			PK:      branch.PrimaryKey,
			Commits: []string{"heads/" + name},
		})
	}
	if len(dargs) == 0 {
		return fmt.Errorf("no branch with file configured. To track a file with a branch, set --set-file and --set-primary-key during commit.")
	}
	return diffMultiple(cmd, c, db, rs, logger, dargs, true, true)
}
