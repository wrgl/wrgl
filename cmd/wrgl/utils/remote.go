// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package utils

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/conf"
)

func MustGetRemote(cmd *cobra.Command, c *conf.Config, name string) *conf.Remote {
	v, ok := c.Remote[name]
	if !ok {
		cmd.PrintErrf("fatal: No such remote '%s'\n", name)
		os.Exit(1)
	}
	return v
}
