// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package config

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/local"
	"github.com/wrgl/core/wrgl/utils"
)

func replaceAllCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replace-all NAME VALUE [VALUE_PATTERN]",
		Short: "Replace all values matching the keys (and optionally the VALUE_PATTERN)",
		Args:  cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			c := openConfigToWrite(cmd, dir)
			v, err := getFieldValue(c, args[0], true)
			if err != nil {
				return err
			}
			if len(args) > 2 {
				idxMap, _, err := filterWithValuePattern(cmd, v, args[2])
				if err != nil {
					return err
				}
				if sl, ok := ToTextSlice(v.Interface()); ok {
					result := []string{}
					n := sl.Len()
					for i := 0; i < n; i++ {
						if _, ok := idxMap[i]; !ok {
							s, err := sl.Get(i)
							if err != nil {
								return err
							}
							result = append(result, s)
						}
					}
					result = append(result, args[1])
					sl, err = TextSliceFromStrSlice(v.Type(), result)
					if err != nil {
						return err
					}
					v.Set(sl.Value)
				} else {
					panic(fmt.Sprintf("type %v does not implement encoding.TextUnmarshaler", v.Type().Elem()))
				}
			} else {
				err = setValue(v, args[1], true)
				if err != nil {
					return err
				}
			}
			return local.SaveConfig(c)
		},
	}
	return cmd
}
