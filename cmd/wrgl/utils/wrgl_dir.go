// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package utils

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wrgl/core/pkg/local"
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
