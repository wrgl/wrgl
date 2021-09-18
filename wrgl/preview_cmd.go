// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/diff"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
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
			cleanup, err := setupDebugLog(cmd)
			if err != nil {
				return err
			}
			defer cleanup()
			cStr := args[0]
			rd := getRepoDir(cmd)
			quitIfRepoDirNotExist(cmd, rd)
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
			tbl, err := objects.GetTable(db, commit.Table)
			if err != nil {
				return fmt.Errorf("GetTable: %v", err)
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
