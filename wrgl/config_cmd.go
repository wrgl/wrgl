package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/config"
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
			file, err := cmd.Flags().GetString("config-file")
			if err != nil {
				return err
			}
			global, err := cmd.Flags().GetBool("global")
			if err != nil {
				return err
			}
			rd := getRepoDir(cmd)
			c, err := config.OpenConfig(global, rd.RootDir, file)
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
			if file == "" && !global && !local {
				c, err = config.AggregateConfig("", rd.RootDir)
				if err != nil {
					return fmt.Errorf("aggregate config error: %v", err)
				}
			}
			return readConfigProp(cmd, c, prop)
		},
	}
	cmd.Flags().Bool("global", false, "Read from/write to global file $XDG_CONFIG_HOME/wrgl/config.yaml.")
	cmd.Flags().Bool("local", false, "Read from/write to file .wrglconfig.yaml. If no flag are set during write then --local is assumed. However if no flag are set during read then all config sources will be aggregated.")
	return cmd
}

func writeConfigProp(cmd *cobra.Command, c *config.Config, prop, val string) error {
	err := SetWithDotNotation(c, prop, val)
	if err != nil {
		return err
	}
	return c.Save()
}

func readConfigProp(cmd *cobra.Command, c *config.Config, prop string) error {
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
