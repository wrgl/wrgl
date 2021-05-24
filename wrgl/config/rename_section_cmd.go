// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package config

import (
	"fmt"
	"reflect"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/wrgl/utils"
)

func renameSectionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rename-section OLD_NAME NEW_NAME",
		Short: "Rename the given section to a new name.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			c := openConfigToWrite(cmd, dir)
			oldField, err := getFieldValue(c, args[0], false)
			if err != nil {
				return err
			}
			newField, err := getFieldValue(c, args[1], true)
			if err != nil {
				return err
			}
			newParent, newName, err := getParentField(c, args[1])
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
			err = unsetField(c, args[0], true)
			if err != nil {
				return err
			}
			return c.Save()
		},
	}
	return cmd
}
