// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package transaction

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/transaction"
)

func startCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Create a new transaction and print the new transaction's id.",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			id, err := transaction.New(db)
			if err != nil {
				return err
			}
			cmd.Println(id.String())
			return nil
		},
	}
	return cmd
}
