package config

import (
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/dotno"
)

func setCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set NAME VALUE",
		Short: "Set value for a field.",
		Long:  "Set value for a field. This command only work with single-valued fields. For multi-valued fields, use \"wrgl config add\" or \"wrgl config replace-all\" instead. For boolean fields, only \"true\" or \"false\" value can be set.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "alter setting in the local config",
				Line:    "wrgl config set receive.denyNonFastForwards true",
			},
			{
				Comment: "alter system-wide config",
				Line:    "wrgl config set pack.maxFileSize 1048576 --system",
			},
			{
				Comment: "alter global config",
				Line:    "wrgl config set user.name \"Jane Lane\" --global",
			},
		}),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			s := writeableConfigStore(cmd, dir)
			c, err := s.Open()
			if err != nil {
				return err
			}
			v, err := dotno.GetFieldValue(c, args[0], true)
			if err != nil {
				return err
			}
			err = dotno.SetValue(v, args[1], false)
			if err != nil {
				return err
			}
			return s.Save(c)
		},
	}
	return cmd
}
