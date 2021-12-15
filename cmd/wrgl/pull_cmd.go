package wrgl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"runtime"
	"sort"
	"strings"

	"github.com/mitchellh/colorstring"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func pullCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pull { BRANCH [REPOSITORY [REFSPEC...]] | --all }",
		Short: "Fetch from and integrate with another repository.",
		Long:  "Fetch from and integrate with another repository. This is shorthand for `wrgl fetch [REPOSITORY [REFSPEC...]]` followed by `wrgl merge BRANCH FETCHED_COMMIT...`",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "pull a branch from remote, setting upstream to the main branch at repo origin",
				Line:    "wrgl pull main origin refs/heads/main:refs/remotes/origin/main --set-upstream",
			},
			{
				Comment: "pull a branch from remote with upstream configured",
				Line:    "wrgl pull main",
			},
			{
				Comment: "pull all branches that have upstream configured",
				Line:    "wrgl pull --all",
			},
		}),
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			wrglDir := utils.MustWRGLDir(cmd)
			s := conffs.NewStore(wrglDir, conffs.AggregateSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			if err := ensureUserSet(cmd, c); err != nil {
				return err
			}
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()

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
			numWorkers, err := cmd.Flags().GetInt("num-workers")
			if err != nil {
				return err
			}
			message, err := cmd.Flags().GetString("message")
			if err != nil {
				return err
			}
			setUpstream, err := cmd.Flags().GetBool("set-upstream")
			if err != nil {
				return err
			}
			all, err := cmd.Flags().GetBool("all")
			if err != nil {
				return err
			}
			ff, err := getFastForward(cmd, c)
			if err != nil {
				return err
			}

			if all {
				names := []string{}
				for name, branch := range c.Branch {
					if branch.Remote == "" {
						continue
					}
					names = append(names, name)
				}
				sort.Strings(names)
				for _, name := range names {
					colorstring.Fprintf(cmd.OutOrStdout(), "pulling [bold]%s[reset]...\n", name)
					if err := pullSingleRepo(cmd, c, db, rs, []string{name}, force, false, noCommit, noGUI, wrglDir, ff, numWorkers, message); err != nil {
						return err
					}
				}
				return nil
			}

			return pullSingleRepo(cmd, c, db, rs, args, force, setUpstream, noCommit, noGUI, wrglDir, ff, numWorkers, message)
		},
	}
	cmd.Flags().BoolP("force", "f", false, "force update local branch in certain conditions.")
	cmd.Flags().Bool("no-commit", false, "perform the merge but don't create a merge commit, instead output merge result to file MERGE_SUM1_SUM2_..._SUMn.csv")
	cmd.Flags().Bool("no-gui", false, "don't show mergetool, instead output conflicts (and resolved rows) to file CONFLICTS_SUM1_SUM2_..._SUMn.csv")
	cmd.Flags().StringP("message", "m", "", "merge commit message")
	cmd.Flags().IntP("num-workers", "n", runtime.GOMAXPROCS(0), "number of CPU threads to utilize (default to GOMAXPROCS)")
	cmd.Flags().BoolP("set-upstream", "u", false, "if the remote is fetched successfully, add upstream (tracking) reference, used by argument-less `wrgl pull`.")
	cmd.Flags().Bool("all", false, "pull all branches that have upstream configured")
	cmd.Flags().Bool("ff", false, "when merging a descendant commit into a branch, don't create a merge commit but simply fast-forward branch to the descendant commit. Create an extra merge commit otherwise. This is the default behavior unless merge.fastForward is configured.")
	cmd.Flags().Bool("no-ff", false, "always create a merge commit, even when a simple fast-forward is possible. This is the default when merge.fastFoward is set to \"never\".")
	cmd.Flags().Bool("ff-only", false, "only allow fast-forward merges. This is the default when merge.fastForward is set to \"only\".")
	return cmd
}

func extractMergeHeads(db objects.Store, rs ref.Store, c *conf.Config, name string, args []string, specs []*conf.Refspec, newBranch bool) (mergeHeads []string, err error) {
	var oldSum []byte
	if !newBranch {
		oldSum, err = ref.GetRef(rs, name)
		if err != nil {
			return nil, err
		}
	}
	if len(args) > 2 {
		// refspecs are specified in arguments
		for _, s := range specs {
			sum, _ := ref.GetRef(rs, strings.TrimPrefix(s.Dst(), "refs/"))
			if sum != nil && !bytes.Equal(sum, oldSum) {
				mergeHeads = append(mergeHeads, s.Dst())
			}
		}
	} else {
		b, ok := c.Branch[args[0]]
		if ok && b.Merge != "" {
			for _, s := range specs {
				if s.Src() == b.Merge {
					sum, _ := ref.GetRef(rs, strings.TrimPrefix(s.Dst(), "refs/"))
					if sum != nil && !bytes.Equal(sum, oldSum) {
						mergeHeads = append(mergeHeads, s.Dst())
						break
					}
				}
			}
		} else if len(specs) > 0 && !specs[0].IsGlob() {
			sum, _ := ref.GetRef(rs, strings.TrimPrefix(specs[0].Dst(), "refs/"))
			if sum != nil && !bytes.Equal(sum, oldSum) {
				mergeHeads = append(mergeHeads, specs[0].Dst())
			}
		}
	}
	return mergeHeads, nil
}

func pullSingleRepo(
	cmd *cobra.Command, c *conf.Config, db objects.Store, rs ref.Store, args []string, force bool,
	setUpstream, noCommit, noGUI bool, wrglDir string, ff conf.FastForward, numWorkers int, message string,
) error {
	newBranch := false
	name, _, _, err := ref.InterpretCommitName(db, rs, args[0], true)
	if err != nil {
		if strings.HasPrefix(err.Error(), "can't find branch ") {
			newBranch = true
			name = args[0]
			if !strings.Contains(name, "/") {
				name = "heads/" + name
			}
		} else {
			return err
		}
	}
	if !strings.HasPrefix(name, "heads/") {
		return fmt.Errorf("%q is not a branch name", args[0])
	}
	remote, rem, specs, err := parseRemoteAndRefspec(cmd, c, name[6:], args[1:])
	if err != nil {
		return err
	}
	cs, err := credentials.NewStore()
	if err != nil {
		return err
	}
	uri, tok, err := getCredentials(cmd, cs, rem.URL)
	if err != nil {
		return err
	}
	err = fetch(cmd, db, rs, c.User, remote, tok, rem, specs, force)
	if err != nil {
		return handleHTTPError(cmd, cs, *uri, err)
	}
	if setUpstream && len(args) > 2 {
		err = setBranchUpstream(cmd, wrglDir, remote, []*Ref{
			{Src: name, Dst: strings.TrimPrefix(specs[0].Src(), "refs/")},
		})
		if err != nil {
			return err
		}
	}

	mergeHeads, err := extractMergeHeads(db, rs, c, name, args, specs, newBranch)
	if err != nil {
		return err
	}
	if len(mergeHeads) == 0 {
		cmd.Println("Already up to date.")
		return nil
	} else if newBranch {
		if len(mergeHeads) > 1 {
			return fmt.Errorf("can't merge more than one reference into a non-existant branch")
		}
		_, sum, com, err := ref.InterpretCommitName(db, rs, mergeHeads[0], true)
		if err != nil {
			return fmt.Errorf("can't get merge head ref: %v", err)
		}
		if err = ref.SaveRef(rs, name, sum, c.User.Name, c.User.Email, "merge", "created from "+mergeHeads[0]); err != nil {
			return err
		}
		cmd.Printf("[%s %s] %s\n", strings.TrimPrefix(name, "heads/"), hex.EncodeToString(sum)[:7], com.Message)
		return nil
	}
	return runMerge(cmd, c, db, rs, append(args[:1], mergeHeads...), noCommit, noGUI, ff, "", numWorkers, message, nil)
}
