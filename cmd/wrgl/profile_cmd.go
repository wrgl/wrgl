// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/fatih/color"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/widgets"
	widgetsprof "github.com/wrgl/wrgl/pkg/widgets/prof"
)

func profileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile COMMIT [--refresh [--ancestors] [--silent]]",
		Short: "Profile data of one or more commits.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "show data profile for branch main",
				Line:    "wrgl profile main",
			},
			{
				Comment: "reprofile data before showing up-to-date data profile",
				Line:    "wrgl profile 092ca64be141ec601fbadc73e4697836 --refresh",
			},
			{
				Comment: "reprofile data for main branch and all ancestor commits",
				Line:    "wrgl profile main --refresh --ancestors",
			},
			{
				Comment: "reprofile data but don't show data profile afterward",
				Line:    "wrgl profile main --refresh --silent",
			},
		}),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			refresh, err := cmd.Flags().GetBool("refresh")
			if err != nil {
				return err
			}
			ancestors, err := cmd.Flags().GetBool("ancestors")
			if err != nil {
				return err
			}
			silent, err := cmd.Flags().GetBool("silent")
			if err != nil {
				return err
			}
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			if err := quitIfRepoDirNotExist(cmd, rd); err != nil {
				return err
			}
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()

			_, sum, commit, err := ref.InterpretCommitName(db, rs, args[0], false)
			if err != nil {
				return err
			}
			if refresh {
				if ancestors {
					cq, err := ref.NewCommitsQueue(db, [][]byte{sum})
					if err != nil {
						return err
					}
					for {
						sum, commit, err := cq.PopInsertParents()
						if err == io.EOF {
							break
						}
						start := time.Now()
						if err = profileTable(db, commit.Table); err != nil {
							return err
						}
						if !silent {
							cmd.Print("profile ")
							color.New(color.FgYellow).Fprint(cmd.OutOrStdout(), hex.EncodeToString(sum)[:7])
							cmd.Printf(" [%s]\n", time.Since(start))
						}
					}
					return nil
				}
				if err = profileTable(db, commit.Table); err != nil {
					return err
				}
			}
			tblProf, err := objects.GetTableProfile(db, commit.Table)
			if err != nil {
				if err = profileTable(db, commit.Table); err != nil {
					return err
				}
				tblProf, err = objects.GetTableProfile(db, commit.Table)
				if err != nil {
					return err
				}
			}
			if !silent {
				return showProfileApp(sum, tblProf)
			}
			return nil
		},
	}
	cmd.Flags().Bool("refresh", false, "recalculate data profile")
	cmd.Flags().Bool("ancestors", false, "when this flag is set together with --refresh, reprofile data for all ancestor")
	cmd.Flags().Bool("silent", false, "when this flag is set together with --refresh, don't show data profile after refreshing")
	return cmd
}

func profileTable(db objects.Store, tblSum []byte) error {
	tbl, err := objects.GetTable(db, tblSum)
	if err != nil {
		return err
	}
	return ingest.ProfileTable(db, tblSum, tbl)
}

func showProfileApp(comSum []byte, tblProf *objects.TableProfile) error {
	app := tview.NewApplication().EnableMouse(true)

	titleBar := tview.NewTextView().SetDynamicColors(true)
	fmt.Fprintf(titleBar, "[yellow]%x[white]  ([teal]%d[white] x [teal]%d[white])", comSum, tblProf.RowsCount, len(tblProf.Columns))

	st := widgetsprof.NewStatTable(tblProf)

	usageBar := widgets.DataTableUsage()

	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(titleBar, 1, 1, false).
		AddItem(st, 0, 1, true).
		AddItem(usageBar, 0, 1, false)

	app.SetBeforeDrawFunc(func(screen tcell.Screen) bool {
		usageBar.BeforeDraw(screen, flex)
		return false
	})

	return app.SetRoot(flex, true).SetFocus(flex).Run()
}
