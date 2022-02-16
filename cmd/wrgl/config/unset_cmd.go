// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package config

import (
	"fmt"
	"reflect"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/dotno"
)

func unsetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unset NAME [VALUE_PATTERN] [--all]",
		Short: "Remove one or more values.",
		Long:  "Remove one or more values. If VALUE_PATTERN is given, the field must be multi-valued. All values matching VALUE_PATTERN will be removed.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "remove an option",
				Line:    "wrgl config unset receive.denyNonFastForwards",
			},
			{
				Comment: "remove the main branch from remote.origin.push",
				Line:    "wrgl config unset remote.origin.push refs/heads/main --fixed-value",
			},
		}),
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			s := writeableConfigStore(cmd, dir)
			c, err := s.Open()
			if err != nil {
				return err
			}
			all, err := cmd.Flags().GetBool("all")
			if err != nil {
				return err
			}
			if len(args) > 1 {
				v, err := dotno.GetFieldValue(c, args[0], false)
				if err != nil {
					return err
				}
				idxMap, _, err := dotno.FilterWithValuePattern(cmd, v, args[1])
				if err != nil {
					return err
				}
				if len(idxMap) > 1 && !all {
					return fmt.Errorf("key contains multiple values, specify flag --all to remove multiple values")
				}
				n := v.Len()
				newLen := n - len(idxMap)
				result := reflect.MakeSlice(v.Type(), newLen, newLen)
				j := 0
				for i := 0; i < n; i++ {
					if _, ok := idxMap[i]; !ok {
						result.Index(j).Set(v.Index(i))
						j++
					}
				}
				v.Set(result)
			} else {
				err := dotno.UnsetField(c, args[0], all)
				if err != nil {
					return err
				}
			}
			return s.Save(c)
		},
	}
	cmd.Flags().Bool("all", false, "remove all values. If VALUE_PATTERN is defined, remove all values that match VALUE_PATTERN.")
	return cmd
}
