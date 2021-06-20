package main

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/wrgl/utils"
)

func pullCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pull BRANCH [REPOSITORY [REFSPEC...]]",
		Short: "Fetch from and integrate with another repository. This is shorthand for `wrgl fetch [REPOSITORY [REFSPEC...]]` followed by `wrgl merge BRANCH FETCHED_COMMIT...`",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			wrglDir := utils.MustWRGLDir(cmd)
			c, err := versioning.AggregateConfig(wrglDir)
			if err != nil {
				return err
			}
			ensureUserSet(cmd, c)
			rd := getRepoDir(cmd)
			db, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			defer db.Close()
			fs := rd.OpenFileStore()

			force, err := cmd.Flags().GetBool("force")
			if err != nil {
				return err
			}
			noCommit, err := cmd.Flags().GetBool("no-commit")
			if err != nil {
				return err
			}
			noGUI, err := cmd.Flags().GetBool("no-gui")
			if err != nil {
				return err
			}
			commitCSV, err := cmd.Flags().GetString("commit-csv")
			if err != nil {
				return err
			}
			numWorkers, err := cmd.Flags().GetInt("num-workers")
			if err != nil {
				return err
			}
			message, err := cmd.Flags().GetString("message")
			if err != nil {
				return err
			}
			pk, err := cmd.Flags().GetStringSlice("primary-key")
			if err != nil {
				return err
			}
			setUpstream, err := cmd.Flags().GetBool("set-upstream")
			if err != nil {
				return err
			}

			name, _, _, err := versioning.InterpretCommitName(db, args[0], true)
			if err != nil {
				return err
			}
			if !strings.HasPrefix(name, "refs/heads/") {
				return fmt.Errorf("%q is not a branch name", args[0])
			}
			remote, rem, specs, err := parseRemoteAndRefspec(cmd, c, args[1:])
			if err != nil {
				return err
			}
			savedRefs, err := fetch(cmd, db, fs, c.User, remote, rem, specs, force)
			if err != nil {
				return err
			}
			if setUpstream && len(args) > 2 && len(savedRefs) > 0 {
				err = setBranchUpstream(cmd, wrglDir, remote, []*Ref{{Src: name, Dst: savedRefs[0].Src}})
				if err != nil {
					return err
				}
			}

			mergeHeads := extractMergeHeads(c, args, savedRefs, specs)
			if len(mergeHeads) == 0 {
				cmd.Println("Already up to date.")
				return nil
			}

			return runMerge(cmd, c, db, fs, append(args[:1], mergeHeads...), noCommit, noGUI, commitCSV, numWorkers, message, pk)
		},
	}
	cmd.Flags().BoolP("force", "f", false, "force update local branch in certain conditions.")
	cmd.Flags().Bool("no-commit", false, "perform the merge but don't create a merge commit, instead output merge result to file MERGE_SUM1_SUM2_..._SUMn.csv")
	cmd.Flags().Bool("no-gui", false, "don't show mergetool, instead output conflicts (and resolved rows) to file CONFLICTS_SUM1_SUM2_..._SUMn.csv")
	cmd.Flags().String("commit-csv", "", "don't perform merge, just create a merge commit with the specified CSV file")
	cmd.Flags().StringP("message", "m", "", "merge commit message")
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "merge commit primary key. This is only used when --commit-csv is in use. If this isn't specified then primary key is the same as BRANCH HEAD's")
	cmd.Flags().IntP("num-workers", "n", runtime.GOMAXPROCS(0), "number of CPU threads to utilize (default to GOMAXPROCS)")
	cmd.Flags().BoolP("set-upstream", "u", false, "if the remote is fetched successfully, add upstream (tracking) reference, used by argument-less `wrgl pull`.")
	return cmd
}

func extractMergeHeads(c *versioning.Config, args []string, savedRefs []*Ref, specs []*versioning.Refspec) []string {
	mergeHeads := []string{}
	if len(args) > 2 {
		// refspecs are specified in arguments
		for _, ref := range savedRefs {
			mergeHeads = append(mergeHeads, ref.Dst)
		}
	} else {
		b, ok := c.Branch[args[0]]
		if ok && b.Merge != "" {
			for _, ref := range savedRefs {
				if ref.Src == b.Merge {
					mergeHeads = append(mergeHeads, ref.Dst)
					break
				}
			}
		} else if len(specs) > 0 && !specs[0].IsGlob() {
			for _, ref := range savedRefs {
				if ref.Dst == specs[0].Dst() {
					mergeHeads = append(mergeHeads, ref.Dst)
					break
				}
			}
		}
	}
	return mergeHeads
}
