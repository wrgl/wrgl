package utils

import (
	"bytes"
	"io"
	"sync"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/wrgl/wrgl/pkg/pbar"
)

var progressBarMutex sync.Mutex

func SetupProgressBarFlags(flags *pflag.FlagSet) {
	flags.Bool("no-progress", false, "don't display progress bar")
}

func getProgressBarContainer(cmd *cobra.Command) (pbar.Container, error) {
	noP, err := cmd.Flags().GetBool("no-progress")
	if err != nil {
		return nil, err
	}
	if noP {
		return pbar.NewNoopContainer(), nil
	}
	return pbar.NewContainer(cmd.OutOrStdout()), nil
}

// WithProgressBar creates a progress bar container while buffering all cmd.Print invocations
func WithProgressBar(cmd *cobra.Command, run func(cmd *cobra.Command, barContainer pbar.Container) error) error {
	progressBarMutex.Lock()
	defer progressBarMutex.Unlock()
	barContainer, err := getProgressBarContainer(cmd)
	if err != nil {
		return err
	}
	defer barContainer.Wait()
	origOut := cmd.OutOrStdout()
	origErr := cmd.ErrOrStderr()
	outW := bytes.NewBuffer(nil)
	errW := bytes.NewBuffer(nil)
	cmd.SetOut(outW)
	cmd.SetErr(errW)
	defer func() {
		io.Copy(origOut, outW)
		io.Copy(origErr, errW)
		cmd.SetOut(origOut)
		cmd.SetErr(origErr)
	}()
	return run(cmd, barContainer)
}
