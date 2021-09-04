// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package config

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/wrgl/utils"
)

func unsetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unset NAME [VALUE_PATTERN]",
		Short: "Remove the value matching the key from config file.",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			s := writeableConfigStore(cmd, dir)
			c, err := s.Open()
			if err != nil {
				return err
			}
			if len(args) > 1 {
				v, err := getFieldValue(c, args[0], false)
				if err != nil {
					return err
				}
				idxMap, _, err := filterWithValuePattern(cmd, v, args[1])
				if err != nil {
					return err
				}
				if len(idxMap) > 1 {
					return fmt.Errorf("key contains multiple values")
				} else if len(idxMap) == 1 {
					sl := MustBeTextSlice(v.Interface())
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
					sl, err = TextSliceFromStrSlice(v.Type(), result)
					if err != nil {
						return err
					}
					v.Set(sl.Value)
				}
			} else {
				err := unsetField(c, args[0], false)
				if err != nil {
					return err
				}
			}
			return s.Save(c)
		},
	}
	return cmd
}
