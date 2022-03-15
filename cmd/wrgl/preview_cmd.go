// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"encoding/hex"
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/diff"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/widgets"
)

func newPreviewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "preview COMMIT",
		Short: "Shows a commit's content in an interactive table.",
		Long:  "Shows a commit's content in an interactive table. To output as a CSV file, use command \"wrgl export\" instead.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "preview the head commit of a branch",
				Line:    "wrgl preview my-branch",
			},
			{
				Comment: "preview an arbitrary commit by specifying the full sum",
				Line:    "wrgl preview 1a2ed6248c7243cdaaecb98ac12213a7",
			},
		}),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cleanup, err := setupDebugLog(cmd)
			if err != nil {
				return err
			}
			defer cleanup()
			cStr := args[0]
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			if err := quitIfRepoDirNotExist(cmd, rd); err != nil {
				return err
			}
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			rs := rd.OpenRefStore()
			defer db.Close()
			_, hash, commit, err := ref.InterpretCommitName(db, rs, cStr, false)
			if err != nil {
				return err
			}
			if commit == nil {
				return fmt.Errorf("commit \"%s\" not found", cStr)
			}
			tbl, err := utils.GetTable(db, rs, commit)
			if err != nil {
				return err
			}
			return previewTable(cmd, db, hex.EncodeToString(hash), commit, tbl)
		},
	}
	return cmd
}

func previewTable(cmd *cobra.Command, db objects.Store, hash string, commit *objects.Commit, tbl *objects.Table) error {
	app := tview.NewApplication().EnableMouse(true)

	// create title bar
	titleBar := tview.NewTextView().SetDynamicColors(true)
	fmt.Fprintf(titleBar, "[yellow]%s[white]  ([teal]%d[white] x [teal]%d[white])", hash, tbl.RowsCount, len(tbl.Columns))

	// create table
	rowReader, err := diff.NewTableReader(db, tbl)
	if err != nil {
		return err
	}
	tv := widgets.NewPreviewTable(rowReader, int(tbl.RowsCount), tbl.Columns, tbl.PK)

	usageBar := widgets.DataTableUsage()

	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(titleBar, 1, 1, false).
		AddItem(tv, 0, 1, true).
		AddItem(usageBar, 0, 1, false)

	app.SetBeforeDrawFunc(func(screen tcell.Screen) bool {
		usageBar.BeforeDraw(screen, flex)
		return false
	})

	return app.SetRoot(flex, true).SetFocus(flex).Run()
}
