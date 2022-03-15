// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wrgl/wrgl/pkg/local"
)

func GetWRGLDir() (string, error) {
	wd := viper.GetString("wrgl_dir")
	if wd != "" {
		return wd, nil
	}
	return local.FindWrglDir()
}

func MustWRGLDir(cmd *cobra.Command) string {
	d, err := GetWRGLDir()
	if err != nil {
		cmd.PrintErrln(err.Error())
		os.Exit(1)
	}
	if d == "" {
		cmd.PrintErrln("Repository not initialized in current directory. Initialize with command:")
		cmd.PrintErrln("  wrgl init")
		os.Exit(1)
	}
	return d
}

func GetRepoDir(cmd *cobra.Command) *local.RepoDir {
	wrglDir := MustWRGLDir(cmd)
	badgerLog, err := cmd.Flags().GetString("badger-log")
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
	rd := local.NewRepoDir(wrglDir, badgerLog)
	return rd
}
