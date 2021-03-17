package main

import (
	_ "embed"

	"github.com/spf13/cobra"
)

//go:embed VERSION
var version string

func newVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Shows binary version",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Printf("WRGL v%s\n", version)
		},
	}
	return cmd
}
