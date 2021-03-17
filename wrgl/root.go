package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "wrgl",
		Short: "Git-like data versioning",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}
	wd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cannot get current working directory.")
		os.Exit(1)
	}
	rootCmd.PersistentFlags().StringP("root-dir", "d", wd, "parent directory of repo, default to current working directory.")
	rootCmd.AddCommand(newInitCmd())
	rootCmd.AddCommand(newConfigCmd())
	rootCmd.AddCommand(newCommitCmd())
	rootCmd.AddCommand(newVersionCmd())
	return rootCmd
}

func execute() error {
	rootCmd := newRootCmd()
	rootCmd.SetOut(os.Stdout)
	return rootCmd.Execute()
}
