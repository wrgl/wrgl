package config

import (
	"fmt"
	"reflect"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/dotno"
)

func addCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add NAME VALUE",
		Short: "Add to a multi-valued field without altering any existing values.",
		Args:  cobra.ExactArgs(2),
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "add new value to remote.origin.push",
				Line:    "wrgl config add remote.origin.push refs/heads/main",
			},
			{
				Comment: "add whole object with JSON string",
				Line:    `wrgl config add auth.clients '{"id": "123", "redirectURIs": ["http://my-client.com"]}'`,
			},
		}),
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
			if v.Kind() != reflect.Slice {
				return fmt.Errorf("command only support multiple values field. Use \"config set\" command instead")
			}
			if err = dotno.AppendSlice(v.Addr(), args[1]); err != nil {
				return err
			}
			return s.Save(c)
		},
	}
	return cmd
}
