package main

import "github.com/spf13/cobra"

func rootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "wrgl-perf",
		Short: "Performance tools for Wrgl repositories",
	}
	cmd.AddCommand(sizeCmd())
	return cmd
}
