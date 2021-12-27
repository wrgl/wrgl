// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"log"
	"os"
	"runtime/pprof"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wrgl/wrgl/cmd/wrgl/auth"
	"github.com/wrgl/wrgl/cmd/wrgl/config"
	"github.com/wrgl/wrgl/cmd/wrgl/credentials"
	"github.com/wrgl/wrgl/cmd/wrgl/hub"
	"github.com/wrgl/wrgl/cmd/wrgl/reflog"
	"github.com/wrgl/wrgl/cmd/wrgl/remote"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
)

func RootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "wrgl",
		Short: "Git-like data versioning",
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
			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
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
	rootCmd.PersistentFlags().String("debug-file", "", "output debug logs to a file")
	rootCmd.PersistentFlags().String("cpuprofile", "", "write cpu profile to file")
	rootCmd.PersistentFlags().String("heapprofile", "", "write heap profile to file")
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
	rootCmd.AddCommand(pullCmd())
	rootCmd.AddCommand(profileCmd())
	rootCmd.AddCommand(config.RootCmd())
	rootCmd.AddCommand(remote.RootCmd())
	rootCmd.AddCommand(reflog.RootCmd())
	rootCmd.AddCommand(credentials.RootCmd())
	rootCmd.AddCommand(auth.RootCmd())
	rootCmd.AddCommand(hub.RootCmd())
	return rootCmd
}

func setupDebugLog(cmd *cobra.Command) (func(), error) {
	r, cleanup, err := utils.SetupDebug(cmd)
	if err != nil {
		return nil, err
	}
	log.SetOutput(r)
	return cleanup, nil
}
