// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package config

import (
	"fmt"
	"reflect"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/dotno"
)

func renameSectionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rename-section OLD_NAME NEW_NAME",
		Short: "Rename a config section.",
		Long:  "Rename a config section. This is equivalent to moving a config section to a different location. Note that the new destination's type must match that of the old location.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "rename a remote",
				Line:    "wrgl config rename-section remote.origin remote.old_origin",
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
			oldField, err := dotno.GetFieldValue(c, args[0], false)
			if err != nil {
				return err
			}
			newField, err := dotno.GetFieldValue(c, args[1], true)
			if err != nil {
				return err
			}
			newParent, newName, err := dotno.GetParentField(c, args[1])
			if err != nil {
				return err
			}
			if oldField.Type() != newField.Type() {
				return fmt.Errorf("types are different: %v != %v", oldField.Type(), newField.Type())
			}
			if newParent.Kind() == reflect.Map {
				newParent.SetMapIndex(reflect.ValueOf(newName), oldField)
			} else {
				newField.Set(oldField)
			}
			err = dotno.UnsetField(c, args[0], true)
			if err != nil {
				return err
			}
			return s.Save(c)
		},
	}
	return cmd
}
