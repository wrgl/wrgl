package config

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
)

func getAllCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-all NAME [VALUE_PATTERN]",
		Short: "Get all values for a multi-valued field.",
		Long:  "Get all values for a multi-valued field. If VALUE_PATTERN is given, then only returns the values that match the pattern (a regular expression if --fixed-value is not set).",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "get all values in field remote.origin.push",
				Line:    "wrgl config get-all remote.origin.push",
			},
			{
				Comment: "get all values in field remote.origin.push that is a branch",
				Line:    "wrgl config get-all remote.origin.push ^refs/heads/",
			},
		}),
		Args: cobra.RangeArgs(1, 2),
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
				return outputValues(cmd, vals, false)
			}
			return outputValues(cmd, v.Interface(), false)
		},
	}
	return cmd
}
