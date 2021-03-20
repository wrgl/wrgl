package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
)

func newPreviewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "preview COMMIT",
		Short: "Shows commit's data in a table view",
		Example: strings.Join([]string{
			`  wrgl preview my-branch`,
			`  wrgl preview 1a2ed6248c7243cdaaecb98ac12213a7`,
		}, "\n"),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cStr := args[0]
			rd := getRepoDir(cmd)
			kvStore, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			hash, commit, err := getCommit(kvStore, cStr)
			if err != nil {
				return err
			}
			ts, err := commit.GetTable(kvStore, rd.OpenFileStore(), seed)
			if err != nil {
				return err
			}
			return previewTable(cmd, hash, commit, ts)
		},
	}
	return cmd
}

func previewTable(cmd *cobra.Command, hash string, commit *versioning.Commit, ts table.Store) error {
	app := tview.NewApplication()

	// create title bar
	titleBar := tview.NewTextView().SetDynamicColors(true)
	nRows, err := ts.NumRows()
	if err != nil {
		return err
	}
	fmt.Fprintf(titleBar, "[yellow]%s[white]  ([teal]%d[white] x [teal]%d[white])", hash, nRows, len(ts.Columns()))

	// create table
	tv := tview.NewTable().SetBorders(false)
	for c, text := range ts.Columns() {
		tv.SetCell(0, c, tview.NewTableCell(text).SetAlign(tview.AlignLeft).SetTextColor(tcell.ColorLightYellow))
	}
	rowReader, err := ts.NewRowReader(0, 80)
	if err != nil {
		return err
	}
	r := 1
	for {
		_, rowContent, err := rowReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		values, err := encoding.DecodeStrings(rowContent)
		if err != nil {
			return err
		}
		for c, text := range values {
			tv.SetCell(r, c, tview.NewTableCell(text).SetAlign(tview.AlignLeft))
		}
		r++
	}
	tv.SetFixed(1, 0)

	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(titleBar, 1, 1, false).
		AddItem(tv, 0, 1, true)
	return app.SetRoot(flex, true).SetFocus(flex).Run()
}
