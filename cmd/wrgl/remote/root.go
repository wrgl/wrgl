// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package remote

import (
	"sort"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remote",
		Short: "Manage remote repositories.",
		Long:  "Manage remote repositories. Running this command without any sub-command prints registered remotes and their URLs.",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			wrglDir := utils.MustWRGLDir(cmd)
			verbose, err := cmd.Flags().GetBool("verbose")
			if err != nil {
				return err
			}
			s := conffs.NewStore(wrglDir, conffs.LocalSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			pairs := [][]string{}
			for k, v := range c.Remote {
				pairs = append(pairs, []string{k, v.URL})
			}
			sort.Slice(pairs, func(i, j int) bool {
				return pairs[i][0] < pairs[j][0]
			})
			for _, p := range pairs {
				if verbose {
					cmd.Printf("%s %s\n", p[0], p[1])
				} else {
					cmd.Println(p[0])
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolP("verbose", "v", false, "print remote url as well")
	cmd.AddCommand(addCmd())
	cmd.AddCommand(renameCmd())
	cmd.AddCommand(removeCmd())
	cmd.AddCommand(setBranchesCmd())
	cmd.AddCommand(getURLCmd())
	cmd.AddCommand(setURLCmd())
	cmd.AddCommand(showCmd())
	return cmd
}
