package main

import (
	"fmt"
	"strings"

	"github.com/gobwas/glob"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/slice"
	"github.com/wrgl/core/pkg/versioning"
)

func newBranchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch [BRANCH_NAME] [START_POINT]",
		Short: "List, create, or delete branches",
		Args:  cobra.MaximumNArgs(2),
		Example: strings.Join([]string{
			`  # list branches`,
			`  wrgl branch`,
			``,
			`  # list branches that match a pattern`,
			`  wrgl branch -l <pattern>`,
			``,
			`  # clone a new branch`,
			`  wrgl branch -c <newbranch> <oldbranch>`,
			``,
			`  # rename a branch`,
			`  wrgl branch -m <newbranch> <oldbranch>`,
			``,
			`  # delete a branch`,
			`  wrgl branch -d <branchname>`,
		}, "\n"),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := getRepoDir(cmd)
			kvStore, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			defer kvStore.Close()

			patterns, err := cmd.Flags().GetStringSlice("list")
			if err != nil {
				return err
			}
			globs := []glob.Glob{}
			for _, pattern := range patterns {
				g, err := glob.Compile(pattern)
				if err != nil {
					return err
				}
				globs = append(globs, g)
			}
			if len(args) == 0 || len(patterns) > 0 {
				return listBranch(cmd, kvStore, globs)
			}

			newBranch, err := cmd.Flags().GetString("copy")
			if err != nil {
				return err
			}
			if newBranch != "" {
				return copyBranch(cmd, kvStore, newBranch, args)
			}

			newBranch, err = cmd.Flags().GetString("move")
			if err != nil {
				return err
			}
			if newBranch != "" {
				return moveBranch(cmd, kvStore, newBranch, args)
			}

			del, err := cmd.Flags().GetBool("delete")
			if err != nil {
				return err
			}
			if del {
				return deleteBranch(cmd, kvStore, args)
			}

			return createBranch(cmd, kvStore, args)
		},
	}
	cmd.Flags().StringSliceP("list", "l", nil, "list branches that match wildcard patterns")
	cmd.Flags().StringP("copy", "c", "", "clone a branch and assign a new name to it")
	cmd.Flags().StringP("move", "m", "", "rename a branch")
	cmd.Flags().BoolP("delete", "d", false, "delete a branch")
	return cmd
}

func listBranch(cmd *cobra.Command, kvStore kv.Store, globs []glob.Glob) error {
	branchMap, err := versioning.ListBranch(kvStore)
	if err != nil {
		return err
	}
	names := []string{}
	for name := range branchMap {
		names = slice.InsertToSortedStringSlice(names, name)
	}
	for _, name := range names {
		if len(globs) > 0 {
			for _, g := range globs {
				if g.Match(name) {
					fmt.Fprintln(cmd.OutOrStdout(), name)
					break
				}
			}
		} else {
			fmt.Fprintln(cmd.OutOrStdout(), name)
		}
	}
	return nil
}

func validateNewBranch(kvStore kv.Store, newBranch string) error {
	if !versioning.BranchPattern.MatchString(newBranch) {
		return fmt.Errorf(`branch name "%s" is invalid`, newBranch)
	}
	_, err := versioning.GetBranch(kvStore, newBranch)
	if err == nil {
		return fmt.Errorf(`branch "%s" already exist`, newBranch)
	}
	return nil
}

func copyBranch(cmd *cobra.Command, kvStore kv.Store, newBranch string, args []string) error {
	err := validateNewBranch(kvStore, newBranch)
	if err != nil {
		return err
	}
	b, err := versioning.GetBranch(kvStore, args[0])
	if err != nil {
		return fmt.Errorf(`branch "%s" does not exist`, args[0])
	}
	err = b.Save(kvStore, newBranch)
	if err != nil {
		return err
	}
	cmd.Printf("created branch %s (%s)\n", newBranch, b.CommitHash)
	return nil
}

func moveBranch(cmd *cobra.Command, kvStore kv.Store, newBranch string, args []string) error {
	err := validateNewBranch(kvStore, newBranch)
	if err != nil {
		return err
	}
	b, err := versioning.GetBranch(kvStore, args[0])
	if err != nil {
		return fmt.Errorf(`branch "%s" does not exist`, args[0])
	}
	err = b.Save(kvStore, newBranch)
	if err != nil {
		return err
	}
	err = versioning.DeleteBranch(kvStore, args[0])
	if err != nil {
		return err
	}
	cmd.Printf("created branch %s (%s)\n", newBranch, b.CommitHash)
	return nil
}

func deleteBranch(cmd *cobra.Command, kvStore kv.Store, args []string) error {
	err := versioning.DeleteBranch(kvStore, args[0])
	if err != nil {
		return err
	}
	cmd.Println("deleted branch", args[0])
	return nil
}

func createBranch(cmd *cobra.Command, kvStore kv.Store, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("please specify both branch name and start point (could be branch name, commit hash)")
	}
	err := validateNewBranch(kvStore, args[0])
	if err != nil {
		return err
	}
	hash, commit, _, err := versioning.InterpretCommitName(kvStore, args[1])
	if err != nil {
		return err
	}
	if commit == nil {
		return fmt.Errorf(`commit "%s" not found`, args[1])
	}
	b := &versioning.Branch{CommitHash: hash}
	err = b.Save(kvStore, args[0])
	if err != nil {
		return err
	}
	cmd.Printf("created branch %s (%s)\n", args[0], b.CommitHash)
	return nil
}
