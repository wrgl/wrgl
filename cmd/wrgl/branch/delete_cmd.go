// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package branch

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/ref"
)

func deleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete BRANCH",
		Short: "delete a branch",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			rs := rd.OpenRefStore()
			return deleteBranch(cmd, rs, args)
		},
	}
	return cmd
}

func deleteBranch(cmd *cobra.Command, rs ref.Store, args []string) error {
	_, err := ref.GetHead(rs, args[0])
	if err != nil {
		return fmt.Errorf(`branch %q does not exist`, args[0])
	}
	err = ref.DeleteHead(rs, args[0])
	if err != nil {
		return err
	}
	cmd.Println("deleted branch", args[0])
	return nil
}
