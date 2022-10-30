// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"context"
	"log"
	"os"

	"github.com/go-logr/logr"
	"github.com/go-logr/stdr"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type loggerKey struct{}

func SetLogger(ctx context.Context, logger *logr.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

func GetLogger(cmd *cobra.Command) *logr.Logger {
	if v := cmd.Context().Value(loggerKey{}); v != nil {
		return v.(*logr.Logger)
	}
	return nil
}

func AddLoggerFlags(flags *pflag.FlagSet) {
	flags.Int("log-verbosity", 0, "log verbosity. Higher value means more log")
	flags.String("log-file", "", "output logs to specified file")
}

func SetupLogger(cmd *cobra.Command) (cleanup func(), err error) {
	if logger := GetLogger(cmd); logger != nil {
		return nil, nil
	}
	verbosity, err := cmd.Flags().GetInt("log-verbosity")
	if err != nil {
		return nil, err
	}
	logFile, err := cmd.Flags().GetString("log-file")
	if err != nil {
		return nil, err
	}
	var _logger stdr.StdLogger
	if logFile != "" {
		f, err := os.Create(logFile)
		if err != nil {
			return nil, err
		}
		_logger = log.New(f, "", log.LstdFlags)
		cleanup = func() {
			f.Close()
		}
	} else {
		_logger = log.New(cmd.OutOrStdout(), "", log.LstdFlags)
	}
	logger := stdr.New(_logger).V(1)
	stdr.SetVerbosity(verbosity)
	cmd.SetContext(SetLogger(cmd.Context(), &logger))
	return cleanup, nil
}
