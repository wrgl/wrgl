package config

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/core/wrgl/utils"
)

func getCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get NAME [VALUE_PATTERN]",
		Short: "Get the value for a given key",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			c := openConfigToRead(cmd, dir)
			v, err := getFieldValue(c, args[0], false)
			if err != nil {
				return err
			}
			if len(args) == 2 {
				_, vals, err := filterWithValuePattern(cmd, v, args[1])
				if err != nil {
					return err
				}
				return outputValues(cmd, vals)
			}
			return outputValues(cmd, v.Interface())
		},
	}
	return cmd
}
