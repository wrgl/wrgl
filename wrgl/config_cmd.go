// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/versioning"
)

func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config PROPERTY [VALUE]",
		Short: "Read or write config",
		Example: strings.Join([]string{
			`  # set user email to "john-doe@domain.com"`,
			`  wrgl config user.email "john-doe@domain.com"`,
			`  # get user email`,
			`  wrgl config user.email`,
			`  # set user name to "John Doe"`,
			`  wrgl config user.name "John Doe"`,
			`  # get user name`,
			`  wrgl config user.name`,
		}, "\n"),
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			prop := args[0]
			global, err := cmd.Flags().GetBool("global")
			if err != nil {
				return err
			}
			rd := getRepoDir(cmd)
			c, err := versioning.OpenConfig(global, rd.FullPath)
			if err != nil {
				return fmt.Errorf("open config error: %v", err)
			}
			if len(args) == 2 {
				return writeConfigProp(cmd, c, prop, args[1])
			}

			local, err := cmd.Flags().GetBool("local")
			if err != nil {
				return err
			}
			if !global && !local {
				c, err = versioning.AggregateConfig(rd.FullPath)
				if err != nil {
					return fmt.Errorf("aggregate config error: %v", err)
				}
			}
			return readConfigProp(cmd, c, prop)
		},
	}
	cmd.Flags().Bool("global", false, "Read from/write to global file $XDG_CONFIG_HOME/wrgl/config.yaml.")
	cmd.Flags().Bool("local", false, "Read from/write to file .wrgl/config.yaml. If no flag are set during write then --local is assumed. However if no flag are set during read then all available files will be aggregated.")
	return cmd
}

func writeConfigProp(cmd *cobra.Command, c *versioning.Config, prop, val string) error {
	err := SetWithDotNotation(c, prop, val)
	if err != nil {
		return err
	}
	return c.Save()
}

func readConfigProp(cmd *cobra.Command, c *versioning.Config, prop string) error {
	v, err := GetWithDotNotation(c, prop)
	if err != nil {
		return fmt.Errorf("config is not set")
	}
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok {
		cmd.Println(s)
	} else if i, ok := v.(int); ok {
		cmd.Println(i)
	} else {
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("parse json error: %v", err)
		}
		cmd.Println(string(b))
	}
	return nil
}
