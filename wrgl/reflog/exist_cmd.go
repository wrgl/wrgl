// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package reflog

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/wrgl/utils"
)

func existCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "exist REF",
		Short: "checks whether a ref has a reflog",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			wrglDir := utils.MustWRGLDir(cmd)
			rd := utils.NewRepoDir(wrglDir, false, false)
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()
			name, _, _, err := ref.InterpretCommitName(db, rs, args[0], true)
			if err != nil {
				return fmt.Errorf("no such ref: %q", args[0])
			}
			if _, err := rs.LogReader(name); err == ref.ErrKeyNotFound {
				return fmt.Errorf("reflog for %q does not exist", args[0])
			}
			cmd.Printf("reflog for %q does exist\n", args[0])
			return nil
		},
	}
	return cmd
}
