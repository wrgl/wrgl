package main

import (
	"fmt"
	"os"

	wrgld "github.com/wrgl/wrgl/wrgld/cmd"
)

func main() {
	rootCmd := wrgld.RootCmd()
	rootCmd.SetOut(os.Stdout)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}
}
