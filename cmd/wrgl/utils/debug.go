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

var logger logr.Logger

func GetLogger() logr.Logger {
	return logger
}

func SetupLogger(cmd *cobra.Command) (cleanup func(), err error) {
	logger = logr.Discard()
	debug, err := cmd.Flags().GetBool("debug")
	if err != nil {
		return func() {}, err
	}
	name, err := cmd.Flags().GetString("debug-file")
	if err != nil {
		return func() {}, err
	}
	var f *os.File
	if name != "" {
		f, err = os.Create(name)
		if err != nil {
			return func() {}, err
		}
		logger = stdr.New(log.New(f, "", 0))
		return func() {
			if f != nil {
				f.Close()
			}
		}, nil
	}
	if debug {
		logger = stdr.New(log.Default())
		return func() {}, nil
	}
	return func() {}, nil
}
