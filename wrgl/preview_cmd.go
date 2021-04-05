package main

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/pkg/widgets"
)

func newPreviewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "preview COMMIT",
		Short: "Shows commit data in a table view",
		Example: strings.Join([]string{
			`  wrgl preview my-branch`,
			`  wrgl preview 1a2ed6248c7243cdaaecb98ac12213a7`,
			`  wrgl preview my-file.csv`,
		}, "\n"),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cStr := args[0]
			rd := getRepoDir(cmd)
			quitIfRepoDirNotExist(cmd, rd)
			kvStore, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			hash, commit, _, err := versioning.InterpretCommitName(kvStore, cStr)
			if err != nil {
				return err
			}
			if commit == nil {
				return fmt.Errorf("commit \"%s\" not found", cStr)
			}
			ts, err := versioning.GetTable(kvStore, rd.OpenFileStore(), seed, commit)
			if err != nil {
				return fmt.Errorf("GetTable: %v", err)
			}
			return previewTable(cmd, hex.EncodeToString(hash), commit, ts)
		},
	}
	return cmd
}

func tableUsageBar() *tview.TextView {
	usageBar := tview.NewTextView().
		SetDynamicColors(true).
		SetWrap(true).
		SetWordWrap(true)
	for _, sl := range [][2]string{
		{"g", "Scroll to begin"},
		{"G", "Scroll to end"},
		{"h", "Left"},
		{"j", "Down"},
		{"k", "Up"},
		{"l", "Right"},
	} {
		fmt.Fprintf(usageBar, "[black:white] %s [white:black] %s\t", sl[0], sl[1])
	}
	return usageBar
}

func previewTable(cmd *cobra.Command, hash string, commit *objects.Commit, ts table.Store) error {
	app := tview.NewApplication().EnableMouse(true)

	// create title bar
	titleBar := tview.NewTextView().SetDynamicColors(true)
	nRows, err := ts.NumRows()
	if err != nil {
		return fmt.Errorf("NumRows: %v", err)
	}
	fmt.Fprintf(titleBar, "[yellow]%s[white]  ([teal]%d[white] x [teal]%d[white])", hash, nRows, len(ts.Columns()))

	// create table
	rowReader, err := ts.NewRowReader()
	if err != nil {
		return fmt.Errorf("NewRowReader: %v", err)
	}
	defer rowReader.Close()
	tv := widgets.NewPreviewTable(rowReader, nRows, ts.Columns(), ts.PrimaryKeyIndices())

	usageBar := tableUsageBar()

	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(titleBar, 1, 1, false).
		AddItem(tv, 0, 1, true).
		AddItem(usageBar, 1, 1, false)
	return app.SetRoot(flex, true).SetFocus(flex).SetBeforeDrawFunc(func(screen tcell.Screen) bool {
		_, _, width, _ := usageBar.GetInnerRect()
		lines := tview.WordWrap(usageBar.GetText(false), width)
		flex.ResizeItem(usageBar, len(lines), 1)
		return false
	}).Run()
}
