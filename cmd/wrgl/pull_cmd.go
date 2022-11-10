// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/fetch"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
	"github.com/wrgl/wrgl/pkg/errors"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/pbar"
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
			if err := utils.EnsureUserSet(cmd, c); err != nil {
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
			logger, cleanup, err := utils.SetupDebug(cmd)
			if err != nil {
				return err
			}
			defer cleanup()
			setUpstream, err := cmd.Flags().GetBool("set-upstream")
			if err != nil {
				return err
			}
			all, err := cmd.Flags().GetBool("all")
			if err != nil {
				return err
			}
			cm, err := utils.NewClientMap()
			if err != nil {
				return err
			}

			if all {
				return pullAll(cmd, c, db, rs, cm, wrglDir, logger)
			}

			if len(args) == 0 {
				return fmt.Errorf("invalid number of arguments. add --help flag to see example usage")
			}

			return utils.WithProgressBar(cmd, false, func(cmd *cobra.Command, barContainer *pbar.Container) error {
				return pullSingleRepo(
					cmd, c, db, rs, cm, args, setUpstream,
					wrglDir, logger, barContainer, nil,
				)
			})
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
	cmd.Flags().Int32P("depth", "d", 0, "The maximum depth pass which commits will be fetched shallowly. Shallow commits only have the metadata but not the data itself. In other words, while you can still see the commit history you cannot access its data. If depth is set to 0 then all missing commits will be fetched in full.")
	cmd.Flags().Bool("ignore-non-existent", false, "ignore branches that cannot be found on remote")
	return cmd
}

func pullAll(
	cmd *cobra.Command,
	c *conf.Config,
	db objects.Store,
	rs ref.Store,
	cm *utils.ClientMap,
	wrglDir string,
	logger *logr.Logger,
) error {

	names := []string{}
	for name, branch := range c.Branch {
		if branch.Remote == "" {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	var reposNotFound = []string{}
	var updateStrs = []string{}
	total := len(names)
	if err := utils.WithProgressBar(cmd, false, func(cmd *cobra.Command, barContainer *pbar.Container) error {
		bar := barContainer.NewBar(int64(total), "Pulling branches", 0)
		defer bar.Abort()
		var updatesCh = make(chan string)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range updatesCh {
				updateStrs = append(updateStrs, s)
			}
		}()
		for _, name := range names {
			if err := pullSingleRepo(
				cmd, c, db, rs, cm, []string{name}, false,
				wrglDir, logger, barContainer, updatesCh,
			); err != nil {
				if errors.Contains(err, `status 404: {"message":"Not Found"}`) {
					reposNotFound = append(reposNotFound, name)
				} else {
					return err
				}
			}
			bar.Incr()
		}
		close(updatesCh)
		wg.Wait()
		return nil
	}); err != nil {
		return err
	}
	for _, name := range reposNotFound {
		cmd.Printf("Skipped repository %q: not found\n", name)
	}
	if n := len(updateStrs); n > 0 {
		cmd.Printf("%d out of %d branches updated\n", n, total)
		for _, update := range updateStrs {
			cmd.Println(update)
		}
	} else {
		cmd.Println("All branches are up-to-date")
	}
	return nil
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
				if s.SrcMatchRef(b.Merge) {
					dst := s.Dst()
					if s.IsGlob() {
						dst = s.DstForRef(b.Merge)
					}
					sum, _ := ref.GetRef(rs, strings.TrimPrefix(dst, "refs/"))
					if sum != nil && !bytes.Equal(sum, oldSum) {
						mergeHeads = append(mergeHeads, dst)
						break
					}
				}
			}
		}
	}
	return mergeHeads, nil
}

func pullSingleRepo(
	cmd *cobra.Command,
	c *conf.Config,
	db objects.Store,
	rs ref.Store,
	cm *utils.ClientMap,
	args []string,
	setUpstream bool,
	wrglDir string,
	logger *logr.Logger,
	pbarContainer *pbar.Container,
	updatesCh chan string,
) (err error) {
	depth, err := cmd.Flags().GetInt32("depth")
	if err != nil {
		return err
	}
	ff, err := getFastForward(cmd, c)
	if err != nil {
		return err
	}
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
		name = "heads/" + args[0]
		newBranch = true
	}
	remote, rem, specs, err := fetch.ParseRemoteAndRefspec(cmd, c, name[6:], args[1:])
	if err != nil {
		return err
	}
	cs, err := credentials.NewStore()
	if err != nil {
		return err
	}
	uri, tok, err := utils.GetCredentials(cmd, cs, rem.URL)
	if err != nil {
		return err
	}
	err = fetch.Fetch(cmd, db, rs, cm, c.User, remote, tok, rem, specs, force, depth, logger, pbarContainer)
	if err != nil {
		return utils.HandleHTTPError(cmd, cs, rem.URL, uri, err)
	}
	if setUpstream && len(args) > 2 {
		ref, err := conf.NewRefspec(name, strings.TrimPrefix(specs[0].Src(), "refs/"), false, false)
		if err != nil {
			return err
		}
		err = setBranchUpstream(cmd, wrglDir, remote, []*conf.Refspec{ref})
		if err != nil {
			return err
		}
	}

	mergeHeads, err := extractMergeHeads(db, rs, c, name, args, specs, newBranch)
	if err != nil {
		return err
	}
	if newBranch {
		if len(mergeHeads) > 1 {
			return fmt.Errorf("can't merge more than one reference into a non-existant branch")
		} else if len(mergeHeads) == 0 {
			ignoreNonExistent, err := cmd.Flags().GetBool("ignore-non-existent")
			if err != nil {
				return err
			}
			if !ignoreNonExistent {
				return fmt.Errorf("nothing to create ref %q from. Make sure the remote branch exists, or use flag --ignore-non-existent to ignore this error", name)
			} else {
				return nil
			}
		}
		_, sum, com, err := ref.InterpretCommitName(db, rs, mergeHeads[0], true)
		if err != nil {
			return fmt.Errorf("can't get merge head ref: %v", err)
		}
		if err = ref.SaveRef(rs, name, sum, c.User.Name, c.User.Email, "pull", "created from "+mergeHeads[0], nil); err != nil {
			return err
		}
		update := fmt.Sprintf("[%s %s] %s", strings.TrimPrefix(name, "heads/"), hex.EncodeToString(sum)[:7], com.Message)
		if updatesCh != nil {
			updatesCh <- update
		}
		if updatesCh == nil {
			cmd.Println()
			cmd.Println(update)
		}
		return nil
	} else if len(mergeHeads) == 0 {
		if updatesCh == nil {
			cmd.Println("Already up to date.")
		}
		return nil
	}
	if err := runMerge(cmd, c, db, rs, append(args[:1], mergeHeads...), noCommit, noGUI, ff, "", numWorkers, message, nil); err != nil {
		return err
	}
	return nil
}
