package utils

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/wrgl/wrgl/pkg/pbar"
)

func SetupProgressBarFlags(flags *pflag.FlagSet) {
	flags.Bool("no-progress", false, "don't display progress bar")
}

func GetProgressBarContainer(cmd *cobra.Command) (pbar.Container, error) {
	noP, err := cmd.Flags().GetBool("no-progress")
	if err != nil {
		return nil, err
	}
	if noP {
		return pbar.NewNoopContainer(), nil
	}
	return pbar.NewContainer(cmd.OutOrStdout()), nil
}
