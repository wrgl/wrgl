// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package remote

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/ref"
)

func showCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show NAME",
		Short: "Prints some information about a remote.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			wrglDir := utils.MustWRGLDir(cmd)
			s := conffs.NewStore(wrglDir, conffs.LocalSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			rem := utils.MustGetRemote(cmd, c, name)
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			rs := rd.OpenRefStore()
			cmd.Printf("* %s\n", name)
			cmd.Printf("  URL: %s\n", rem.URL)

			if len(rem.Fetch) > 0 {
				cmd.Println("  Fetch:")
				for _, rs := range rem.Fetch {
					cmd.Printf("    %s\n", rs)
				}
			}

			if len(rem.Push) > 0 {
				cmd.Println("  Push:")
				for _, rs := range rem.Push {
					cmd.Printf("    %s\n", rs)
				}
			}

			refs, err := ref.ListRemoteRefs(rs, name)
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
