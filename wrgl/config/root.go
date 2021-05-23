// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package config

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/versioning"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Read or write config",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}
	cmd.PersistentFlags().Bool("system", false, "For writing commands: write to system-wide /usr/local/etc/wrgl/config.yaml rather than the repository .wrgl/config.yaml. For reading commands: read only from global file /usr/local/etc/wrgl/config.yaml rather than from all available files.")
	cmd.PersistentFlags().Bool("global", false, "For writing commands: write to global $XDG_CONFIG_HOME/wrgl/config.yaml rather than the repository .wrgl/config.yaml. For reading commands: read only from global $XDG_CONFIG_HOME/wrgl/config.yaml rather than from all available files.")
	cmd.PersistentFlags().Bool("local", false, "For writing commands: write to file .wrgl/config.yaml. This is the default behavior. For reading commands: read only from the repository .wrgl/config.yaml rather than from all available files.")
	cmd.PersistentFlags().StringP("file", "f", "", "Use the given config file instead of .wrgl/config.yaml")
	cmd.PersistentFlags().Bool("fixed-value", false, "When used with the VALUE_PATTERN argument, treat VALUE_PATTERN as an exact string instead of a regular expression.")
	cmd.PersistentFlags().BoolP("null", "z", false, "For all options that output values and/or keys, always end values with the null character (instead of a newline). Use newline instead as a delimiter between key and value. This allows for secure parsing of the output without getting confused e.g. by values that contain line breaks.")
	// cmd.Flags().StringSlice("add", nil, "Adds a new value to the option without altering any existing values.")
	cmd.AddCommand(getCmd())
	cmd.AddCommand(setCmd())
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

func openConfigToRead(cmd *cobra.Command, rootDir string) (c *versioning.Config) {
	file, system, global, local := fileOptions(cmd)
	var err error
	if local {
		c, err = versioning.OpenConfig(false, false, rootDir, "")
		if err != nil {
			fatal(cmd, err)
		}
		return
	} else if file == "" && !system && !global {
		c, err = versioning.AggregateConfig(rootDir)
		if err != nil {
			fatal(cmd, err)
		}
		return
	}
	c, err = versioning.OpenConfig(system, global, rootDir, file)
	if err != nil {
		fatal(cmd, err)
	}
	return
}

func openConfigToWrite(cmd *cobra.Command, rootDir string) (c *versioning.Config) {
	file, system, global, _ := fileOptions(cmd)
	c, err := versioning.OpenConfig(system, global, rootDir, file)
	if err != nil {
		fatal(cmd, err)
	}
	return
}

func filterWithValuePattern(cmd *cobra.Command, v reflect.Value, valuePattern string) (idxs []int, vals []string, err error) {
	fixedValue, err := cmd.Flags().GetBool("fixed-value")
	if err != nil {
		return
	}
	if v.Kind() != reflect.Slice || v.Elem().Kind() != reflect.String {
		err = fmt.Errorf("VALUE_PATTERN should only be specified for options that accept multiple strings")
		return
	}
	sl := v.Interface().([]string)
	if fixedValue {
		for i, s := range sl {
			if s == valuePattern {
				idxs = append(idxs, i)
				vals = append(vals, s)
			}
		}
	} else {
		pat, err := regexp.Compile(valuePattern)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid VALUE_PATTERN: %v", err)
		}
		for i, s := range sl {
			if pat.MatchString(s) {
				idxs = append(idxs, i)
				vals = append(vals, s)
			}
		}
	}
	return
}

func outputValues(cmd *cobra.Command, vals interface{}) (err error) {
	null, err := cmd.Flags().GetBool("null")
	if err != nil {
		return
	}
	if sl, ok := vals.([]string); ok {
		if null {
			cmd.Print(strings.Join(sl, "\x00"), "\x00")
		} else {
			cmd.Println(strings.Join(sl, "\n"))
		}
	} else {
		if null {
			cmd.Printf("%+v\x00", vals)
		} else {
			cmd.Printf("%+v\n", vals)
		}
	}
	return nil
}
