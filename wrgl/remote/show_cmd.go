// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package remote

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	"github.com/wrgl/core/pkg/local"
	"github.com/wrgl/core/pkg/ref"
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
			s := conffs.NewStore(wrglDir, conffs.LocalSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			rem := utils.MustGetRemote(cmd, c, name)
			rd := local.NewRepoDir(wrglDir, false, false)
			rs := rd.OpenRefStore()
			cmd.Printf("* %s\n", name)
			cmd.Printf("  URL: %s\n", rem.URL)

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
