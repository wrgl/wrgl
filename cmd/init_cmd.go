package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/spf13/cobra"
)

func newInitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init REPONAME",
		Short: "Initialize repository with specified name",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			reponame := args[0]
			rd, err := cmd.Flags().GetString("root-dir")
			if err != nil {
				return err
			}
			useBigTableStore, err := cmd.Flags().GetBool("use-big-table-store")
			if err != nil {
				return err
			}
			return initRepo(cmd, rd, reponame, useBigTableStore)
		},
	}
	cmd.Flags().BoolP("use-big-table-store", "b", false, "use big table store if CSV which can't fit in RAM. Only taken into account for the first commit.")
	return cmd
}

func initRepo(cmd *cobra.Command, rootDir, reponame string, useBigTableStore bool) error {
	pat := regexp.MustCompile(`^[-_0-9a-zA-Z]+$`)
	if !pat.MatchString(reponame) {
		return fmt.Errorf("invalid repo name, must consist of only alphanumeric letters, hyphen and underscore")
	}
	rd := &repoDir{
		rootDir: rootDir,
		name:    reponame,
	}
	err := rd.Init(useBigTableStore)
	if err != nil {
		return err
	}
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	p, err := filepath.Rel(wd, rd.fullPath())
	if err != nil {
		return err
	}
	cmd.Printf("Created directory %s\n", p)
	return nil
}
