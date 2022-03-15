// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gobwas/glob"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/slice"
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
			`  # create a new branch`,
			`  wrgl branch <newbranch> <ref or commit sum>`,
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
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			wrglDir := utils.MustWRGLDir(cmd)
			s := conffs.NewStore(wrglDir, conffs.AggregateSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			if err := quitIfRepoDirNotExist(cmd, rd); err != nil {
				return err
			}
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()

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
				return listBranch(cmd, rs, globs)
			}

			newBranch, err := cmd.Flags().GetString("copy")
			if err != nil {
				return err
			}
			if newBranch != "" {
				return copyBranch(cmd, c.User, rs, args[0], newBranch)
			}

			newBranch, err = cmd.Flags().GetString("move")
			if err != nil {
				return err
			}
			if newBranch != "" {
				return moveBranch(cmd, rs, args[0], newBranch)
			}

			del, err := cmd.Flags().GetBool("delete")
			if err != nil {
				return err
			}
			if del {
				return deleteBranch(cmd, rs, args)
			}

			return createBranch(cmd, c.User, db, rs, args)
		},
	}
	cmd.Flags().StringSliceP("list", "l", nil, "list branches that match wildcard patterns")
	cmd.Flags().StringP("copy", "c", "", "clone a branch and assign a new name to it")
	cmd.Flags().StringP("move", "m", "", "rename a branch")
	cmd.Flags().BoolP("delete", "d", false, "delete a branch")
	return cmd
}

func listBranch(cmd *cobra.Command, rs ref.Store, globs []glob.Glob) error {
	branchMap, err := ref.ListHeads(rs)
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

func validateNewBranch(rs ref.Store, newBranch string) error {
	if !ref.HeadPattern.MatchString(newBranch) {
		return fmt.Errorf(`branch name "%s" is invalid`, newBranch)
	}
	_, err := ref.GetHead(rs, newBranch)
	if err == nil {
		return fmt.Errorf(`branch "%s" already exist`, newBranch)
	}
	return nil
}

func copyBranch(cmd *cobra.Command, u *conf.User, rs ref.Store, oldBranch, newBranch string) error {
	err := validateNewBranch(rs, newBranch)
	if err != nil {
		return err
	}
	b, err := ref.GetHead(rs, oldBranch)
	if err != nil {
		return fmt.Errorf(`branch %q does not exist`, oldBranch)
	}
	_, err = ref.CopyRef(rs, "heads/"+oldBranch, "heads/"+newBranch)
	if err != nil {
		return err
	}
	cmd.Printf("created branch %s (%s)\n", newBranch, hex.EncodeToString(b))
	return nil
}

func moveBranch(cmd *cobra.Command, rs ref.Store, oldBranch, newBranch string) error {
	err := validateNewBranch(rs, newBranch)
	if err != nil {
		return err
	}
	_, err = ref.GetHead(rs, oldBranch)
	if err != nil {
		return fmt.Errorf(`branch %q does not exist`, oldBranch)
	}
	sum, err := ref.RenameRef(rs, "heads/"+oldBranch, "heads/"+newBranch)
	if err != nil {
		return err
	}
	cmd.Printf("created branch %s (%s)\n", newBranch, hex.EncodeToString(sum))
	return nil
}

func deleteBranch(cmd *cobra.Command, rs ref.Store, args []string) error {
	_, err := ref.GetHead(rs, args[0])
	if err != nil {
		return fmt.Errorf(`branch %q does not exist`, args[0])
	}
	err = ref.DeleteHead(rs, args[0])
	if err != nil {
		return err
	}
	cmd.Println("deleted branch", args[0])
	return nil
}

func createBranch(cmd *cobra.Command, u *conf.User, db objects.Store, rs ref.Store, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("please specify both branch name and start point (could be branch name, commit hash)")
	}
	err := validateNewBranch(rs, args[0])
	if err != nil {
		return err
	}
	name, hash, commit, err := ref.InterpretCommitName(db, rs, args[1], false)
	if err != nil {
		return err
	}
	if commit == nil {
		return fmt.Errorf(`commit "%s" not found`, args[1])
	}
	name = strings.TrimPrefix(name, "heads/")
	err = ref.SaveRef(rs, "heads/"+args[0], hash, u.Name, u.Email, "branch", "created from "+name)
	if err != nil {
		return err
	}
	cmd.Printf("created branch %s (%s)\n", args[0], hex.EncodeToString(hash))
	return nil
}
