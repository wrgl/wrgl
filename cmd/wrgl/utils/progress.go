package utils

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/wrgl/wrgl/pkg/pbar"
)

var barContainer *pbar.Container

func SetupProgressBarFlags(flags *pflag.FlagSet) {
	flags.Bool("no-progress", false, "don't display progress bar")
}

func getProgressBarContainer(cmd *cobra.Command, quiet bool) (*pbar.Container, error) {
	noP, err := cmd.Flags().GetBool("no-progress")
	if err != nil {
		return nil, err
	}
	return pbar.NewContainer(cmd.OutOrStdout(), noP || quiet), nil
}

// WithProgressBar creates a progress bar container while buffering all cmd.Print invocations
func WithProgressBar(cmd *cobra.Command, quiet bool, run func(cmd *cobra.Command, barContainer *pbar.Container) error) (err error) {
	if barContainer == nil {
		barContainer, err = getProgressBarContainer(cmd, quiet)
		if err != nil {
			return err
		}
		defer func() {
			barContainer.Wait()
			barContainer = nil
		}()
	} else {
		restore := barContainer.OverideQuiet(quiet)
		defer restore()
	}
	return run(cmd, barContainer)
}
