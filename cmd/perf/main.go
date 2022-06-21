package main

import (
	"fmt"
	"os"
)

func main() {
	cmd := rootCmd()
	cmd.SetErr(os.Stderr)
	cmd.SetOut(os.Stdout)
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
