// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package branch

import (
	"fmt"

	"github.com/gobwas/glob"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/slice"
)

func listCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list [PATTERN...]",
		Short: "List branches",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			rs := rd.OpenRefStore()
			globs := []glob.Glob{}
			for _, pattern := range args {
				g, err := glob.Compile(pattern)
				if err != nil {
					return err
				}
				globs = append(globs, g)
			}
			return listBranch(cmd, rs, globs)
		},
	}
	return cmd
}

func listBranch(cmd *cobra.Command, rs ref.Store, globs []glob.Glob) error {
	branchMap, err := ref.ListHeads(rs)
	if err != nil {
		return err
	}
	names := []string{}
	for name := range branchMap {
		names = slice.InsertToSortedStringSlice(names, name)
	}
	for _, name := range names {
		if len(globs) > 0 {
			for _, g := range globs {
				if g.Match(name) {
					fmt.Fprintln(cmd.OutOrStdout(), name)
					break
				}
			}
		} else {
			fmt.Fprintln(cmd.OutOrStdout(), name)
		}
	}
	return nil
}
