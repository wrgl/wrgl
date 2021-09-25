// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wrgl/core/pkg/local"
)

func newInitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize repository in current directory",
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
			rd := local.NewRepoDir(dir, false, false)
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
