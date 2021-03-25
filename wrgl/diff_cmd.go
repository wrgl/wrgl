package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/diff"
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
			`  wrgl diff branch-1 branch-1^`,
			`  wrgl diff branch-1 branch-2`,
			`  wrgl diff branch-1 1a2ed6248c7243cdaaecb98ac12213a7`,
			`  wrgl diff 1a2ed6248c7243cdaaecb98ac12213a7 f1cf51efa2c1e22843b0e083efd89792`,
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
	return cmd
}

var hashPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

func getCommit(db kv.DB, cStr string) (string, *versioning.Commit, error) {
	var hash = cStr
	if !hashPattern.MatchString(cStr) {
		branch, err := versioning.GetBranch(db, cStr)
		if err != nil {
			return "", nil, err
		}
		hash = branch.CommitHash
	}
	commit, err := versioning.GetCommit(db, hash)
	if err != nil {
		return "", nil, err
	}
	return hash, commit, nil
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

func outputDiffToTerminal(cmd *cobra.Command, db kv.DB, commitHash1, commitHash2 string, diffChan <-chan diff.Diff) error {
	var (
		cols, oldCols, pk []string
		addedRowReader    *table.KeyListRowReader
		removedRowReader  *table.KeyListRowReader
		addedTable        *widgets.BufferedTable
		removedTable      *widgets.BufferedTable
	)

	titleBar := tview.NewTextView().SetDynamicColors(true)
	fmt.Fprintf(titleBar, "[yellow]%s[white] vs [yellow]%s[white]", commitHash1, commitHash2)

	pBar := widgets.NewProgressBar("Comparing...")

	tabPages := widgets.NewTabPages()

	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(titleBar, 1, 1, false).
		AddItem(pBar, 1, 1, false).
		AddItem(tabPages, 0, 1, true)
	app := tview.NewApplication().SetRoot(flex, true).SetFocus(flex)

	for event := range diffChan {
		switch event.Type {
		case diff.Init:
			cols, oldCols, pk = event.Columns, event.OldColumns, event.PK
		case diff.Progress:
			if event.Total == event.Progress {
				flex.RemoveItem(pBar)
			} else {
				pBar.SetTotal(event.Total)
				pBar.SetCurrent(event.Progress)
			}
		case diff.PrimaryKey:
			_, addedCols, removedCols := slice.CompareStringSlices(cols, oldCols)
			if len(addedCols) > 0 {
				tabPages.AddTab(
					fmt.Sprintf("+%d columns", len(addedCols)),
					widgets.NewColumnsList(nil, addedCols, nil),
				)
			}
			if len(removedCols) > 0 {
				tabPages.AddTab(
					fmt.Sprintf("-%d columns", len(removedCols)),
					widgets.NewColumnsList(nil, nil, removedCols),
				)
			}
			unchanged, added, removed := slice.CompareStringSlices(pk, event.OldPK)
			if len(added) > 0 || len(removed) > 0 {
				tabPages.AddTab(
					"Primary key",
					widgets.NewColumnsList(unchanged, added, removed),
				)
			}
		case diff.RowAdd:
			if addedRowReader == nil {
				addedRowReader = table.NewKeyListRowReader(db, []string{event.Row})
				pkIndices, err := slice.KeyIndices(cols, pk)
				if err != nil {
					return err
				}
				addedTable = widgets.NewBufferedTable(addedRowReader, 1, cols, pkIndices)
				tabPages.AddTab("+1 rows", addedTable)
			} else {
				addedRowReader.Add(event.Row)
				addedTable.SetRowCount(addedRowReader.NumRows())
				tabPages.SetLabel(addedTable, fmt.Sprintf("+%d rows", addedRowReader.NumRows()))
			}
		case diff.RowRemove:
			if removedRowReader == nil {
				removedRowReader = table.NewKeyListRowReader(db, []string{event.Row})
				pkIndices, err := slice.KeyIndices(oldCols, pk)
				if err != nil {
					return err
				}
				removedTable = widgets.NewBufferedTable(removedRowReader, 1, oldCols, pkIndices)
				tabPages.AddTab("-1 rows", removedTable)
			} else {
				removedRowReader.Add(event.Row)
				removedTable.SetRowCount(removedRowReader.NumRows())
				tabPages.SetLabel(removedTable, fmt.Sprintf("-%d rows", removedRowReader.NumRows()))
			}
		}
	}
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
	commitHash1, commit1, err := getCommit(kvStore, cStr1)
	if err != nil {
		return err
	}
	commitHash2, commit2, err := getCommit(kvStore, cStr2)
	if err != nil {
		return err
	}
	ts1, err := commit1.GetTable(kvStore, fs, seed)
	if err != nil {
		return err
	}
	ts2, err := commit2.GetTable(kvStore, fs, seed)
	if err != nil {
		return err
	}
	errChan := make(chan error, 10)
	diffChan := diff.DiffTables(ts1, ts2, 65*time.Millisecond, errChan)
	if format == diffFormatJSON {
		inflatedChan := diff.Inflate(kvStore, diffChan, errChan)
		err := outputDiffToJSON(cmd, inflatedChan)
		if err != nil {
			return err
		}
	} else {
		err := outputDiffToTerminal(cmd, kvStore, commitHash1, commitHash2, diffChan)
		if err != nil {
			return err
		}
	}
	close(errChan)
	err, ok := <-errChan
	if ok {
		return err
	}
	return nil
}
