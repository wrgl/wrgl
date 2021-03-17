package main

import (
	"encoding/json"
	"strings"

	"github.com/spf13/cobra"
)

func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config PROPERTY [VALUE]",
		Short: "Read or write config",
		Example: strings.Join([]string{
			`  wrgl config user.email "john-doe@domain.com"`,
			`  wrgl config user.email`,
			`  wrgl config user.name "John Doe"`,
			`  wrgl config user.name`,
		}, "\n"),
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			prop := args[0]
			file, err := cmd.Flags().GetString("file")
			if err != nil {
				return err
			}
			global, err := cmd.Flags().GetBool("global")
			if err != nil {
				return err
			}
			c, err := openConfig(global, file)
			if err != nil {
				return err
			}
			if len(args) == 2 {
				return writeConfigProp(cmd, c, prop, args[1])
			}
			return readConfigProp(cmd, c, prop)
		},
	}
	cmd.Flags().Bool("global", false, "Read from/write to global file $XDG_CONFIG_HOME/wrgl/config.yaml.")
	cmd.Flags().Bool("local", true, "Read from/write to file .wrglconfig.yaml. This is the default behavior.")
	cmd.Flags().StringP("file", "f", "", "Use the given config file instead.")
	return cmd
}

func writeConfigProp(cmd *cobra.Command, c *Config, prop, val string) error {
	err := SetWithDotNotation(c, prop, val)
	if err != nil {
		return err
	}
	return c.Save()
}

func readConfigProp(cmd *cobra.Command, c *Config, prop string) error {
	v, err := GetWithDotNotation(c, prop)
	if err != nil {
		return err
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
			return err
		}
		cmd.Println(string(b))
	}
	return nil
}
