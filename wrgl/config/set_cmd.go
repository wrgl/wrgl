package config

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/wrgl/utils"
)

func setCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set NAME VALUE [VALUE_PATTERN]",
		Short: "Set the value for a given key",
		Args:  cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			c := openConfigToWrite(cmd, dir)
			v, err := getFieldValue(c, args[0], true)
			if err != nil {
				return err
			}
			err = setValue(v, args[1], false)
			if err != nil {
				return err
			}
			return utils.SaveConfig(c)
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
