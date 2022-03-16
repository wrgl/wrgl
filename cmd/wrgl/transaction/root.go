// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package transaction

import "github.com/spf13/cobra"

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "transaction",
		Short: "Manage transactions.",
		Long:  "Manage transactions. A transaction is a group of commits that either all persisted to their respective branch or all aborted.",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}
	cmd.AddCommand(startCmd())
	cmd.AddCommand(commitCmd())
	cmd.AddCommand(discardCmd())
	return cmd
}
