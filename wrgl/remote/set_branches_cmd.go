// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package remote

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/conf"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	"github.com/wrgl/core/wrgl/utils"
)

func setBranchesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-branches NAME BRANCH",
		Short: "Changes the list of branches tracked by the named remote.",
		Long:  "This can be used to track a subset of the available remote branches after the initial setup for a remote.",
		Args:  cobra.ExactArgs(2),
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
