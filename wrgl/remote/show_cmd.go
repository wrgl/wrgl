// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package remote

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/wrgl/utils"
)

func showCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show NAME",
		Short: "Gives some information about the remote NAME",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			wrglDir := utils.MustWRGLDir(cmd)
			c, err := versioning.OpenConfig(false, false, wrglDir, "")
			if err != nil {
				return err
			}
			rem := utils.MustGetRemote(cmd, c, name)
			rd := versioning.NewRepoDir(wrglDir, false, false)
			db, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			defer db.Close()
			cmd.Printf("* %s\n", name)
			cmd.Printf("  URL: %s\n", rem.URL)

			refs, err := versioning.ListRemoteRefs(db, name)
			if err != nil {
				return err
			}
			if len(refs) > 0 {
				cmd.Println("  Remote branches:")
				rows := [][]string{}
				for k := range refs {
					if rem.FetchDstMatchRef(fmt.Sprintf("refs/remotes/%s/%s", name, k)) {
						rows = append(rows, []string{k, "tracked"})
					} else {
						rows = append(rows, []string{k})
					}
				}
				sort.Slice(rows, func(i, j int) bool {
					return rows[i][0] < rows[j][0]
				})
				utils.PrintTable(cmd.OutOrStdout(), rows, 4)
			}
			return nil
		},
	}
	return cmd
}
