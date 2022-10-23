// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"log"
	"os"

	"github.com/go-logr/logr"
	"github.com/go-logr/stdr"
	"github.com/spf13/cobra"
)

// SetupDebug open debug file (specified with --debug-file) for writing
func SetupDebug(cmd *cobra.Command) (l *logr.Logger, cleanup func(), err error) {
	debug, err := cmd.Flags().GetBool("debug")
	if err != nil {
		return nil, nil, err
	}
	name, err := cmd.Flags().GetString("debug-file")
	if err != nil {
		return nil, nil, err
	}
	var f *os.File
	if name != "" {
		f, err = os.Create(name)
		if err != nil {
			return nil, nil, err
		}
		logger := stdr.New(log.New(f, "", 0))
		return &logger, func() {
			if f != nil {
				f.Close()
			}
		}, nil
	}
	if debug {
		logger := stdr.New(log.Default())
		return &logger, func() {}, nil
	}
	return nil, func() {}, nil
}
