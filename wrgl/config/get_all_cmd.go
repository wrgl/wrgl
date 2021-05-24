package config

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/wrgl/utils"
)

func getAllCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-all NAME [VALUE_PATTERN]",
		Short: "Like get, but returns all values for a multi-valued key.",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			c := openConfigToRead(cmd, dir)
			v, err := getFieldValue(c, args[0], false)
			if err != nil {
				return fmt.Errorf("key %q is not set", args[0])
			}
			if len(args) == 2 {
				_, vals, err := filterWithValuePattern(cmd, v, args[1])
				if err != nil {
					return err
				}
				return outputValues(cmd, vals, false)
			}
			return outputValues(cmd, v.Interface(), false)
		},
	}
	return cmd
}
