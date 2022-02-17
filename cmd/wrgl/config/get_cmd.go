package config

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/dotno"
)

func getCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get NAME [VALUE_PATTERN]",
		Short: "Get value of a field.",
		Long:  "Get value of a field. If VALUE_PATTERN is given then only return the value if it matches pattern (a regular expression if --fixed-value is not set). Returns error code 1 if the key was not found.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "get current user email",
				Line:    "wrgl config get user.email",
			},
			{
				Comment: "get the last branch reference in field remote.origin.push",
				Line:    "wrgl config get remote.origin.push ^refs/heads/",
			},
			{
				Comment: "print an object as JSON string",
				Line:    "wrgl config get remote.origin",
			},
			{
				Comment: "get all the push refspec",
				Line:    "wrgl config get remote.origin.push",
			},
			{
				Comment: "get the second push refspec",
				Line:    "wrgl config get remote.origin.push.1",
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
			v, err := dotno.GetFieldValue(c, args[0], false)
			if err != nil {
				return fmt.Errorf("key %q is not set", args[0])
			}
			if len(args) == 2 {
				_, vals, err := dotno.FilterWithValuePattern(cmd, v, args[1])
				if err != nil {
					return err
				}
				return dotno.OutputValues(cmd, vals)
			}
			return dotno.OutputValues(cmd, v.Interface())
		},
	}
	return cmd
}
