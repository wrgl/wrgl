// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package config

import (
	"reflect"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/dotno"
)

func replaceAllCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replace-all NAME VALUE [VALUE_PATTERN]",
		Short: "Replace all values with a single value.",
		Long:  "Replace all values with a single value. If VALUE_PATTERN is given, only replace the values matching it.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "replace all values under remote.origin.push with refs/heads/main",
				Line:    "wrgl config replace-all remote.origin.push refs/heads/main",
			},
			{
				Comment: "replace all branches under remote.origin.push with refs/heads/main",
				Line:    "wrgl config replace-all remote.origin.push refs/heads/main ^refs/heads/",
			},
		}),
		Args: cobra.RangeArgs(2, 3),
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
			if len(args) > 2 {
				idxMap, _, err := dotno.FilterWithValuePattern(cmd, v, args[2])
				if err != nil {
					return err
				}
				n := v.Len()
				newLen := n - len(idxMap) + 1
				result := reflect.MakeSlice(v.Type(), newLen, newLen)
				j := 0
				for i := 0; i < n; i++ {
					if _, ok := idxMap[i]; !ok {
						result.Index(j).Set(v.Index(i))
						j++
					}
				}
				if err := dotno.SetValue(result.Index(j), args[1]); err != nil {
					return err
				}
				v.Set(result)
			} else {
				result := reflect.MakeSlice(v.Type(), 1, 1)
				if err := dotno.SetValue(result.Index(0), args[1]); err != nil {
					return err
				}
				v.Set(result)
			}
			return s.Save(c)
		},
	}
	return cmd
}
