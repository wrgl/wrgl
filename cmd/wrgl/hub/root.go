// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package hub

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/hub/repo"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hub",
		Short: "Interacts with WrglHub",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}
	cmd.AddCommand(repo.RootCmd())
	return cmd
}
