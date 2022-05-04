// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"log"
	"os"

	"github.com/spf13/cobra"
)

// SetupDebug open debug file (specified with --debug-file) for writing
func SetupDebug(cmd *cobra.Command) (l *log.Logger, cleanup func(), err error) {
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
		return log.New(f, "", 0), func() {
			if f != nil {
				f.Close()
			}
		}, nil
	}
	if debug {
		return log.Default(), func() {}, nil
	}
	return nil, func() {}, nil
}
