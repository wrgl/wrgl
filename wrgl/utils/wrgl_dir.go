// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package utils

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func GetWRGLDir() (string, error) {
	wd := viper.GetString("wrgl_dir")
	if wd != "" {
		return wd, nil
	}
	d, err := filepath.Abs(".")
	if err != nil {
		return "", err
	}
	home, _ := os.UserHomeDir()
	if home != "" && !strings.HasPrefix(d, home) {
		home = ""
	}
	for {
		wd = filepath.Join(d, ".wrgl")
		_, err := os.Stat(wd)
		if err == nil {
			return wd, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		if home != "" {
			if d == home {
				break
			}
		} else if filepath.Dir(d) == d {
			break
		}
		d = filepath.Dir(d)
	}
	return "", nil
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
