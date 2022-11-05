// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"os"
	"runtime/pprof"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wrgl/wrgl/cmd/wrgl/branch"
	"github.com/wrgl/wrgl/cmd/wrgl/config"
	"github.com/wrgl/wrgl/cmd/wrgl/credentials"
	"github.com/wrgl/wrgl/cmd/wrgl/fetch"
	"github.com/wrgl/wrgl/cmd/wrgl/reflog"
	"github.com/wrgl/wrgl/cmd/wrgl/remote"
	"github.com/wrgl/wrgl/cmd/wrgl/transaction"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
)

var version string

func RootCmd() *cobra.Command {
	var cleanupLogger func()
	rootCmd := &cobra.Command{
		Use:     "wrgl",
		Short:   "Git-like data versioning",
		Version: version,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			cpuprofile, err := cmd.Flags().GetString("cpuprofile")
			if err != nil {
				return err
			}
			if cpuprofile != "" {
				f, err := os.Create(cpuprofile)
				if err != nil {
					return err
				}
				pprof.StartCPUProfile(f)
			}
			cleanupLogger, err = utils.SetupLogger(cmd)
			if err != nil {
				return err
			}
			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			if cleanupLogger != nil {
				cleanupLogger()
			}
			pprof.StopCPUProfile()
			heapprofile, err := cmd.Flags().GetString("heapprofile")
			if err != nil {
				return err
			}
			if heapprofile != "" {
				f, err := os.Create(heapprofile)
				if err != nil {
					return err
				}
				defer f.Close()
				err = pprof.WriteHeapProfile(f)
				if err != nil {
					return err
				}
			}
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
	rootCmd.PersistentFlags().String("badger-log", "", `set Badger log level, valid options are "error", "warning", "debug", and "info" (defaults to "error")`)
	rootCmd.PersistentFlags().String("cpuprofile", "", "write cpu profile to file")
	rootCmd.PersistentFlags().String("heapprofile", "", "write heap profile to file")
	utils.AddLoggerFlags(rootCmd.PersistentFlags())
	rootCmd.AddCommand(newInitCmd())
	rootCmd.AddCommand(newCommitCmd())
	rootCmd.AddCommand(newLogCmd())
	rootCmd.AddCommand(newPreviewCmd())
	rootCmd.AddCommand(newDiffCmd())
	rootCmd.AddCommand(newExportCmd())
	rootCmd.AddCommand(branch.RootCmd())
	rootCmd.AddCommand(newPruneCmd())
	rootCmd.AddCommand(newResetCmd())
	rootCmd.AddCommand(newCatObjCmd())
	rootCmd.AddCommand(fetch.RootCmd())
	rootCmd.AddCommand(newPushCmd())
	rootCmd.AddCommand(mergeCmd())
	rootCmd.AddCommand(pullCmd())
	rootCmd.AddCommand(profileCmd())
	rootCmd.AddCommand(config.RootCmd())
	rootCmd.AddCommand(remote.RootCmd())
	rootCmd.AddCommand(reflog.RootCmd())
	rootCmd.AddCommand(credentials.RootCmd())
	rootCmd.AddCommand(transaction.RootCmd())
	rootCmd.AddCommand(gcCmd())
	rootCmd.AddCommand(reapplyCmd())
	return rootCmd
}
