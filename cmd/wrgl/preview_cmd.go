// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/google/uuid"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/diff"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/widgets"
)

func newPreviewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "preview { COMMIT_HASH | BRANCH | FILE_NAME }",
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
			{
				Comment: "preview a file. Only works if the entire fit in memory",
				Line:    "wrgl preview data.csv",
			},
		}),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			if err := quitIfRepoDirNotExist(cmd, rd); err != nil {
				return err
			}
			var db objects.Store
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			rs := rd.OpenRefStore()
			defer db.Close()
			txid, err := cmd.Flags().GetString("txid")
			if err != nil {
				return err
			}
			pk, err := cmd.Flags().GetStringSlice("primary-key")
			if err != nil {
				return err
			}
			delim, err := utils.GetRuneFromFlag(cmd, "delimiter")
			if err != nil {
				return err
			}
			var sum []byte
			var commit *objects.Commit
			if txid != "" {
				sum, commit, err = getCommitWithTxid(db, rs, txid, args[0])
				if err != nil {
					return err
				}
			} else {
				_, sum, commit, err = ref.InterpretCommitName(db, rs, args[0], false)
				if err != nil {
					if strings.HasPrefix(err.Error(), "can't find ") {
						memStore := objmock.NewStore()
						file, err := os.Open(args[0])
						if err != nil {
							return err
						}
						sum, commit, err = createInMemCommit(cmd, memStore, pk, file, false, delim)
						if err != nil {
							file.Close()
							return err
						}
						file.Close()
						db = memStore
					} else {
						return err
					}
				}
			}
			if commit == nil {
				return fmt.Errorf("commit \"%s\" not found", args[0])
			}
			tbl, err := utils.GetTable(db, rs, commit)
			if err != nil {
				return err
			}
			return previewTable(cmd, db, hex.EncodeToString(sum), commit, tbl)
		},
	}
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "field names to be used as primary key (only applicable if preview target is a file)")
	cmd.Flags().String("delimiter", "", "CSV delimiter to use when preview target is a file. Defaults to comma.")
	cmd.Flags().String("txid", "", "preview commit with specified transaction id. COMMIT must be a branch name.")
	return cmd
}

func getCommitWithTxid(db objects.Store, rs ref.Store, txid, branch string) (sum []byte, commit *objects.Commit, err error) {
	tid, err := uuid.Parse(txid)
	if err != nil {
		err = fmt.Errorf("error parsing txid: %v", err)
		return
	}
	r := ref.TransactionRef(tid.String(), branch)
	sum, err = ref.GetRef(rs, r)
	if err != nil {
		err = fmt.Errorf("ref %q not found", r)
		return
	}
	commit, err = objects.GetCommit(db, sum)
	if err != nil {
		err = fmt.Errorf("commit %x not found", sum)
		return
	}
	return sum, commit, nil
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
