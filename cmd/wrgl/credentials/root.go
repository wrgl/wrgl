// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package credentials

import "github.com/spf13/cobra"

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "credentials",
		Short: "Manage credentials",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}
	cmd.AddCommand(printCmd())
	cmd.AddCommand(listCmd())
	cmd.AddCommand(removeCmd())
	cmd.AddCommand(authenticateCmd())
	return cmd
}
