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
			if len(args) > 2 {
				idxs, _, err := filterWithValuePattern(cmd, v, args[2])
				if err != nil {
					return err
				}
				sl := v.Interface().([]string)
				for _, i := range idxs {
					sl[i] = args[1]
				}
				v.Set(reflect.ValueOf(sl))
			} else {
				err = setValue(v, args[1])
				if err != nil {
					return err
				}
			}
			return c.Save()
		},
	}
	return cmd
}

func setValue(v reflect.Value, val string) error {
	switch v.Kind() {
	case reflect.String:
		v.Set(reflect.ValueOf(val))
	case reflect.Ptr:
		if v.Elem().Kind() == reflect.Bool {
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
			panic(fmt.Sprintf("setValue: unhandled elem kind %v", v.Elem().Kind()))
		}
	default:
		panic(fmt.Sprintf("setValue: unhandled kind %v", v.Kind()))
	}
	return nil
}
