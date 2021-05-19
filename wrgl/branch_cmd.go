// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gobwas/glob"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/slice"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/wrgl/utils"
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
			wrglDir := utils.MustWRGLDir(cmd)
			conf, err := versioning.AggregateConfig(wrglDir)
			if err != nil {
				return err
			}
			quitIfRepoDirNotExist(cmd, rd)
			kvStore, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			defer kvStore.Close()
			fs := rd.OpenFileStore()

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
				return copyBranch(cmd, conf.User, kvStore, fs, args[0], newBranch)
			}

			newBranch, err = cmd.Flags().GetString("move")
			if err != nil {
				return err
			}
			if newBranch != "" {
				return moveBranch(cmd, kvStore, fs, args[0], newBranch)
			}

			del, err := cmd.Flags().GetBool("delete")
			if err != nil {
				return err
			}
			if del {
				return deleteBranch(cmd, kvStore, fs, args)
			}

			return createBranch(cmd, conf.User, kvStore, fs, args)
		},
	}
	cmd.Flags().StringSliceP("list", "l", nil, "list branches that match wildcard patterns")
	cmd.Flags().StringP("copy", "c", "", "clone a branch and assign a new name to it")
	cmd.Flags().StringP("move", "m", "", "rename a branch")
	cmd.Flags().BoolP("delete", "d", false, "delete a branch")
	return cmd
}

func listBranch(cmd *cobra.Command, kvStore kv.Store, globs []glob.Glob) error {
	branchMap, err := versioning.ListHeads(kvStore)
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

func validateNewBranch(db kv.DB, newBranch string) error {
	if !versioning.HeadPattern.MatchString(newBranch) {
		return fmt.Errorf(`branch name "%s" is invalid`, newBranch)
	}
	_, err := versioning.GetHead(db, newBranch)
	if err == nil {
		return fmt.Errorf(`branch "%s" already exist`, newBranch)
	}
	return nil
}

func copyBranch(cmd *cobra.Command, u *versioning.ConfigUser, db kv.DB, fs kv.FileStore, oldBranch, newBranch string) error {
	err := validateNewBranch(db, newBranch)
	if err != nil {
		return err
	}
	b, err := versioning.GetHead(db, oldBranch)
	if err != nil {
		return fmt.Errorf(`branch %q does not exist`, oldBranch)
	}
	_, err = versioning.CopyRef(db, fs, "heads/"+oldBranch, "heads/"+newBranch)
	if err != nil {
		return err
	}
	cmd.Printf("created branch %s (%s)\n", newBranch, hex.EncodeToString(b))
	return nil
}

func moveBranch(cmd *cobra.Command, db kv.DB, fs kv.FileStore, oldBranch, newBranch string) error {
	err := validateNewBranch(db, newBranch)
	if err != nil {
		return err
	}
	_, err = versioning.GetHead(db, oldBranch)
	if err != nil {
		return fmt.Errorf(`branch %q does not exist`, oldBranch)
	}
	sum, err := versioning.RenameRef(db, fs, "heads/"+oldBranch, "heads/"+newBranch)
	if err != nil {
		return err
	}
	cmd.Printf("created branch %s (%s)\n", newBranch, hex.EncodeToString(sum))
	return nil
}

func deleteBranch(cmd *cobra.Command, db kv.DB, fs kv.FileStore, args []string) error {
	_, err := versioning.GetHead(db, args[0])
	if err != nil {
		return fmt.Errorf(`branch %q does not exist`, args[0])
	}
	err = versioning.DeleteHead(db, fs, args[0])
	if err != nil {
		return err
	}
	cmd.Println("deleted branch", args[0])
	return nil
}

func createBranch(cmd *cobra.Command, u *versioning.ConfigUser, db kv.DB, fs kv.FileStore, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("please specify both branch name and start point (could be branch name, commit hash)")
	}
	err := validateNewBranch(db, args[0])
	if err != nil {
		return err
	}
	name, hash, commit, err := versioning.InterpretCommitName(db, args[1], false)
	if err != nil {
		return err
	}
	if commit == nil {
		return fmt.Errorf(`commit "%s" not found`, args[1])
	}
	name = strings.TrimPrefix(name, "refs/heads/")
	err = versioning.SaveRef(db, fs, "heads/"+args[0], hash, u.Name, u.Email, "branch", "created from "+name)
	if err != nil {
		return err
	}
	cmd.Printf("created branch %s (%s)\n", args[0], hex.EncodeToString(hash))
	return nil
}
