package config

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/wrgl/utils"
)

func getCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get NAME [VALUE_PATTERN]",
		Short: "Get the value for a given key. Returns error code 1 if the key was not found and the last value if multiple key values were found.",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			s := readableConfigStore(cmd, dir)
			c, err := s.Open()
			if err != nil {
				return err
			}
			v, err := getFieldValue(c, args[0], false)
			if err != nil {
				return fmt.Errorf("key %q is not set", args[0])
			}
			if len(args) == 2 {
				_, vals, err := filterWithValuePattern(cmd, v, args[1])
				if err != nil {
					return err
				}
				return outputValues(cmd, vals, true)
			}
			return outputValues(cmd, v.Interface(), true)
		},
	}
	return cmd
}
