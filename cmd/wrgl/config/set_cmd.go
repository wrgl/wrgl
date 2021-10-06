package config

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
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
			v, err := getFieldValue(c, args[0], true)
			if err != nil {
				return err
			}
			err = setValue(v, args[1], false)
			if err != nil {
				return err
			}
			return s.Save(c)
		},
	}
	return cmd
}

func setValue(v reflect.Value, val string, setMultiple bool) error {
	switch v.Kind() {
	case reflect.String:
		v.Set(reflect.ValueOf(val))
	case reflect.Ptr:
		if v.Type().Elem().Kind() == reflect.Bool {
			yes := true
			no := false
			if strings.ToLower(val) == "true" {
				v.Set(reflect.ValueOf(&yes))
			} else if strings.ToLower(val) == "false" {
				v.Set(reflect.ValueOf(&no))
			} else {
				return fmt.Errorf("bad value: %q, only accept %q or %q", val, "true", "false")
			}
		} else {
			panic(fmt.Sprintf("setValue: unhandled pointer of type %v", v.Type().Elem()))
		}
	case reflect.Slice:
		if _, ok := ToTextSlice(v.Interface()); ok {
			if setMultiple {
				sl, err := TextSliceFromStrSlice(v.Type(), []string{val})
				if err != nil {
					return err
				}
				v.Set(sl.Value)
			} else {
				return fmt.Errorf("more than one value for this key")
			}
		} else {
			panic(fmt.Sprintf("setValue: unhandled slice of type %v", v.Type().Elem()))
		}
	default:
		panic(fmt.Sprintf("setValue: unhandled type %v", v.Type()))
	}
	return nil
}
