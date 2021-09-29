// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wrgl/core/cmd/wrgl/utils"
)

func newInitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize a repository in current directory",
		Long:  "Initialize a repository in current directory. The repository will live under <current directory>/.wrgl.",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			wd, err := os.Getwd()
			if err != nil {
				return err
			}
			dir := viper.GetString("wrgl_dir")
			create := false
			if dir == "" {
				dir = filepath.Join(wd, ".wrgl")
				create = true
			}
			_, err = os.Stat(dir)
			if err == nil {
				cmd.Printf("Repository already initialized at %s\n", dir)
				return nil
			}
			rd := utils.GetRepoDir(cmd)
			err = rd.Init()
			if err != nil {
				return err
			}
			if create {
				cmd.Println("Repository initialized at .wrgl")
			}
			return nil
		},
	}
	return cmd
}
