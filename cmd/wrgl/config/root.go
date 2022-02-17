// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package config

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Read or write config.",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}
	cmd.PersistentFlags().Bool("system", false, "for writing commands: write to system-wide /usr/local/etc/wrgl/config.yaml rather than the repository .wrgl/config.yaml. For reading commands: read only from global file /usr/local/etc/wrgl/config.yaml rather than from all available files.")
	cmd.PersistentFlags().Bool("global", false, "for writing commands: write to global $XDG_CONFIG_HOME/wrgl/config.yaml rather than the repository .wrgl/config.yaml. For reading commands: read only from global $XDG_CONFIG_HOME/wrgl/config.yaml rather than from all available files.")
	cmd.PersistentFlags().Bool("local", false, "for writing commands: write to file .wrgl/config.yaml. This is the default behavior. For reading commands: read only from the repository .wrgl/config.yaml rather than from all available files.")
	cmd.PersistentFlags().StringP("file", "f", "", "use the given config file instead of .wrgl/config.yaml")
	cmd.PersistentFlags().Bool("fixed-value", false, "when used with the VALUE_PATTERN argument, treat VALUE_PATTERN as an exact string instead of a regular expression.")
	cmd.PersistentFlags().BoolP("null", "z", false, "for all options that output values and/or keys, always end values with the null character (instead of a newline). Use newline instead as a delimiter between key and value. This allows for secure parsing of the output without getting confused e.g. by values that contain line breaks.")
	cmd.AddCommand(getCmd())
	cmd.AddCommand(setCmd())
	cmd.AddCommand(replaceAllCmd())
	cmd.AddCommand(addCmd())
	cmd.AddCommand(unsetCmd())
	cmd.AddCommand(renameSectionCmd())
	return cmd
}

func fatal(cmd *cobra.Command, err error) {
	cmd.PrintErrln(err.Error())
	os.Exit(1)
}

func fileOptions(cmd *cobra.Command) (file string, system, global, local bool) {
	file, err := cmd.Flags().GetString("file")
	if err != nil {
		fatal(cmd, err)
	}
	system, err = cmd.Flags().GetBool("system")
	if err != nil {
		fatal(cmd, err)
	}
	global, err = cmd.Flags().GetBool("global")
	if err != nil {
		fatal(cmd, err)
	}
	local, err = cmd.Flags().GetBool("local")
	if err != nil {
		fatal(cmd, err)
	}
	return
}

func readableConfigStore(cmd *cobra.Command, rootDir string) (s conf.Store) {
	file, system, global, l := fileOptions(cmd)
	source := conffs.AggregateSource
	if system {
		source = conffs.SystemSource
	} else if global {
		source = conffs.GlobalSource
	} else if l {
		source = conffs.LocalSource
	} else if file != "" {
		source = conffs.FileSource
	}
	return conffs.NewStore(rootDir, source, file)
}

func writeableConfigStore(cmd *cobra.Command, rootDir string) (s conf.Store) {
	file, system, global, _ := fileOptions(cmd)
	source := conffs.LocalSource
	if system {
		source = conffs.SystemSource
	} else if global {
		source = conffs.GlobalSource
	} else if file != "" {
		source = conffs.FileSource
	}
	return conffs.NewStore(rootDir, source, file)
}
