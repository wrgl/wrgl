// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package config

import (
	"reflect"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/wrgl/utils"
)

func unsetAllCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unset-all NAME [VALUE_PATTERN]",
		Short: "Remove all values matching the key from config file.",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			c := openConfigToWrite(cmd, dir)
			if len(args) > 1 {
				v, err := getFieldValue(c, args[0], false)
				if err != nil {
					return err
				}
				idxMap, _, err := filterWithValuePattern(cmd, v, args[1])
				if err != nil {
					return err
				}
				sl := v.Interface().([]string)
				if len(idxMap) == len(sl) {
					err = unsetField(c, args[0], true)
					if err != nil {
						return err
					}
				} else {
					result := []string{}
					for i, s := range sl {
						if _, ok := idxMap[i]; !ok {
							result = append(result, s)
						}
					}
					v.Set(reflect.ValueOf(result))
				}
			} else {
				err := unsetField(c, args[0], true)
				if err != nil {
					return err
				}
			}
			return c.Save()
		},
	}
	return cmd
}
