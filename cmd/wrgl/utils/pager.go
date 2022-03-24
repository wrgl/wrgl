// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"io"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
)

func getPager(cmd *cobra.Command) (*exec.Cmd, io.WriteCloser, error) {
	pager := os.Getenv("PAGER")
	if pager == "" {
		pager = "less"
	}
	p := exec.Command(pager)
	out, err := p.StdinPipe()
	if err != nil {
		return nil, nil, err
	}
	p.Stdout = cmd.OutOrStdout()
	p.Stderr = cmd.ErrOrStderr()
	if err := p.Start(); err != nil {
		return nil, nil, err
	}
	return p, out, nil
}

func PagerOrOut(cmd *cobra.Command) (io.Writer, func(), error) {
	noPager, err := cmd.Flags().GetBool("no-pager")
	if err != nil {
		return nil, nil, err
	}
	if noPager {
		return cmd.OutOrStdout(), func() {}, nil
	}
	pager, writer, err := getPager(cmd)
	if err != nil {
		return nil, nil, err
	}
	return writer, func() {
		writer.Close()
		pager.Wait()
	}, nil
}
