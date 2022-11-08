// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package transaction

import (
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/transaction"
)

func commitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "commit TRANSACTION_ID",
		Short: "Commit a transaction.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()
			id, err := uuid.Parse(args[0])
			if err != nil {
				return fmt.Errorf("error parsing transaction id: %v", err)
			}
			commits, err := transaction.Commit(db, rs, id)
			if err != nil {
				return fmt.Errorf("error committing transaction: %v", err)
			}
			for refname, com := range commits {
				cmd.Printf("[%s %s] %s\n", refname, hex.EncodeToString(com.Sum)[:7], com.Message)
			}
			return nil
		},
	}
	return cmd
}
