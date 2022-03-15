// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/pkg/conf"
)

func EnsureUserSet(cmd *cobra.Command, c *conf.Config) error {
	if c.User == nil || c.User.Email == "" {
		return fmt.Errorf(strings.Join([]string{
			"User config not set. Set your user config with these commands:",
			"",
			`  wrgl config set --global user.email "john-doe@domain.com"`,
			`  wrgl config set --global user.name "John Doe"`,
		}, "\n"))
	}
	return nil
}
