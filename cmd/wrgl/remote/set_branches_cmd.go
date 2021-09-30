// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package remote

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/cmd/wrgl/utils"
	"github.com/wrgl/core/pkg/conf"
	conffs "github.com/wrgl/core/pkg/conf/fs"
)

func setBranchesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-branches NAME BRANCH",
		Short: "Set the list of branches tracked by this remote.",
		Long:  "Set the list of branches tracked by this remote. By default, this command replaces the refspec list found in remote.<remote>.fetch.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "track only branch main, and nothing else",
				Line:    "wrgl remote set-branches origin main",
			},
			{
				Comment: "track branch main, in addition to everything else in remote.<remote>.fetch",
				Line:    "wrgl remote set-branches origin main --add",
			},
		}),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			branch := args[1]
			add, err := cmd.Flags().GetBool("add")
			if err != nil {
				return err
			}
			wrglDir := utils.MustWRGLDir(cmd)
			s := conffs.NewStore(wrglDir, conffs.LocalSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			rem := utils.MustGetRemote(cmd, c, name)
			refspec := conf.MustParseRefspec(
				fmt.Sprintf("+refs/heads/%s:refs/remotes/%s/%s", branch, name, branch),
			)
			if add {
				rem.Fetch = append(rem.Fetch, refspec)
			} else {
				rem.Fetch = []*conf.Refspec{refspec}
			}
			return s.Save(c)
		},
	}
	cmd.Flags().Bool("add", false, "instead of replacing the list of currently tracked branches, adds to that list.")
	return cmd
}
