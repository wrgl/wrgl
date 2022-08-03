// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package main

import (
	"fmt"
	"os"

	"github.com/wrgl/wrgl/cmd/wrgl"
)

func main() {
	rootCmd := wrgl.RootCmd()
	rootCmd.SetOut(os.Stdout)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}
}
