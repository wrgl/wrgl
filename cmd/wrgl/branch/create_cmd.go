// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package branch

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func createCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create BRANCH { { --copy | --move } BRANCH | COMMIT }",
		Short: "Create a new branch",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "create a new branch from commit sum",
				Line:    "wrgl branch create my-branch 1234567890abcdef1234567890abcdef",
			},
			{
				Comment: "create a new branch from an existing branch",
				Line:    "wrgl branch create my-branch my-other-branch",
			},
			{
				Comment: "create a new branch from arbitrary ref",
				Line:    "wrgl branch create my-branch refs/remotes/origin/my-branch",
			},
			{
				Comment: "copy a branch and its reflogs",
				Line:    "wrgl branch create my-branch --copy my-other-branch",
			},
			{
				Comment: "rename a branch",
				Line:    "wrgl branch create my-branch --move my-other-branch",
			},
		}),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()
			s := conffs.NewStore(rd.FullPath, conffs.AggregateSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			copyF, err := cmd.Flags().GetBool("copy")
			if err != nil {
				return err
			}
			move, err := cmd.Flags().GetBool("move")
			if err != nil {
				return err
			}
			if copyF {
				return copyBranch(cmd, c.User, rs, args[1], args[0])
			}
			if move {
				return moveBranch(cmd, rs, args[1], args[0])
			}
			return createBranch(cmd, c.User, db, rs, args)
		},
	}
	cmd.Flags().BoolP("copy", "c", false, "copy a branch and its reflogs")
	cmd.Flags().BoolP("move", "m", false, "rename a branch")
	return cmd
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
	err = ref.SaveRef(rs, "heads/"+args[0], hash, u.Name, u.Email, "branch", "created from "+name, nil)
	if err != nil {
		return err
	}
	cmd.Printf("created branch %s (%s)\n", args[0], hex.EncodeToString(hash))
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
