package config

import (
	"fmt"
	"reflect"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/wrgl/utils"
)

func addCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add NAME VALUE",
		Short: "Add a new value to the option without altering any existing values.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			c := openConfigToWrite(cmd, dir)
			v, err := getFieldValue(c, args[0], true)
			if err != nil {
				return err
			}
			err = addValue(v, args[1])
			if err != nil {
				return err
			}
			return c.Save()
		},
	}
	return cmd
}

func addValue(v reflect.Value, val string) error {
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("command only support multiple values field. Use \"config set\" command instead")
	}
	if sl, ok := ToTextSlice(v.Interface()); ok {
		err := sl.Append(val)
		if err != nil {
			return err
		}
		v.Set(sl.Value)
	} else {
		panic(fmt.Sprintf("type %v does not implement encoding.TextUnmarshaler and encoding.TextMarshaler", v.Type().Elem()))
	}
	return nil
}
