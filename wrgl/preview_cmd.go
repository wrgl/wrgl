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
			closeDebugFile := setupDebug(cmd)
			defer closeDebugFile()
			cStr := args[0]
			rd := getRepoDir(cmd)
			quitIfRepoDirNotExist(cmd, rd)
			kvStore, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			fs := rd.OpenFileStore()
			defer kvStore.Close()
			_, hash, commit, err := versioning.InterpretCommitName(kvStore, cStr, false)
			if err != nil {
				return err
			}
			if commit == nil {
				return fmt.Errorf("commit \"%s\" not found", cStr)
			}
			ts, err := table.ReadTable(kvStore, fs, commit.Table)
			if err != nil {
				return fmt.Errorf("GetTable: %v", err)
			}
			return previewTable(cmd, hex.EncodeToString(hash), commit, ts)
		},
	}
	return cmd
}

func previewTable(cmd *cobra.Command, hash string, commit *objects.Commit, ts table.Store) error {
	app := tview.NewApplication().EnableMouse(true)

	// create title bar
	titleBar := tview.NewTextView().SetDynamicColors(true)
	nRows := ts.NumRows()
	fmt.Fprintf(titleBar, "[yellow]%s[white]  ([teal]%d[white] x [teal]%d[white])", hash, nRows, len(ts.Columns()))

	// create table
	rowReader := ts.NewRowReader()
	tv := widgets.NewPreviewTable(rowReader, nRows, ts.Columns(), ts.PrimaryKeyIndices())

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
