package main

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/versioning"
)

func newInitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize repository with specified name",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd, err := cmd.Flags().GetString("root-dir")
			if err != nil {
				return err
			}
			return initRepo(cmd, rd)
		},
	}
	return cmd
}

func initRepo(cmd *cobra.Command, rootDir string) error {
	rd := versioning.NewRepoDir(rootDir, false, false)
	err := rd.Init()
	if err != nil {
		return err
	}
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	p, err := filepath.Rel(wd, rd.FullPath())
	if err != nil {
		return err
	}
	cmd.Printf("Created directory %s\n", p)
	return nil
}
