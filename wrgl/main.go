package main

import (
	"fmt"
	"os"
)

// Version is semver of WRGL
var Version string

func main() {
	if err := execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}
}