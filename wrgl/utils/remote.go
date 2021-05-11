package utils

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/versioning"
)

func MustGetRemote(cmd *cobra.Command, c *versioning.Config, name string) *versioning.ConfigRemote {
	v, ok := c.Remote[name]
	if !ok {
		cmd.PrintErrf("fatal: No such remote '%s'\n", name)
		os.Exit(1)
	}
	return v
}
