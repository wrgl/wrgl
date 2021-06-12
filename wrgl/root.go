// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wrgl/core/wrgl/config"
	"github.com/wrgl/core/wrgl/reflog"
	"github.com/wrgl/core/wrgl/remote"
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
	viper.SetEnvPrefix("")
	rootCmd.PersistentFlags().String("wrgl-dir", "", "parent directory of repo, default to current working directory.")
	viper.BindEnv("wrgl_dir")
	viper.BindPFlag("wrgl_dir", rootCmd.PersistentFlags().Lookup("wrgl-dir"))
	rootCmd.PersistentFlags().Bool("badger-log-info", false, "set Badger log level to INFO")
	rootCmd.PersistentFlags().Bool("badger-log-debug", false, "set Badger log level to DEBUG")
	rootCmd.PersistentFlags().String("debug-file", "", "name of file to print debug logs to")
	rootCmd.AddCommand(newInitCmd())
	rootCmd.AddCommand(newCommitCmd())
	rootCmd.AddCommand(newVersionCmd())
	rootCmd.AddCommand(newLogCmd())
	rootCmd.AddCommand(newPreviewCmd())
	rootCmd.AddCommand(newDiffCmd())
	rootCmd.AddCommand(newExportCmd())
	rootCmd.AddCommand(newBranchCmd())
	rootCmd.AddCommand(newPruneCmd())
	rootCmd.AddCommand(newResetCmd())
	rootCmd.AddCommand(newCatFileCmd())
	rootCmd.AddCommand(newFetchCmd())
	rootCmd.AddCommand(newPushCmd())
	rootCmd.AddCommand(mergeCmd())
	rootCmd.AddCommand(config.RootCmd())
	rootCmd.AddCommand(remote.RootCmd())
	rootCmd.AddCommand(reflog.RootCmd())
	return rootCmd
}

func setupDebug(cmd *cobra.Command) func() error {
	name, err := cmd.Flags().GetString("debug-file")
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
	if name == "" {
		return nil
	}
	f, err := os.Create(name)
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
	log.SetOutput(f)
	return f.Close
}

func execute() error {
	rootCmd := newRootCmd()
	rootCmd.SetOut(os.Stdout)
	return rootCmd.Execute()
}
