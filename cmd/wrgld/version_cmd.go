// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgld

import (
	_ "embed"

	"github.com/spf13/cobra"
)

//go:embed VERSION
var version string

func newVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Shows version",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Printf("WRGLD v%s\n", version)
		},
	}
	return cmd
}
