// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/cenkalti/backoff/v4"
	"github.com/mitchellh/colorstring"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/fetch"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/pbar"
	"github.com/wrgl/wrgl/pkg/ref"
)

func newPushCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "push { REPOSITORY [REFSPEC...] | --all }",
		Short: "Updates remote refs using local refs, sending objects necessary to complete the given refs.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "push changes of two branches to origin",
				Line:    "wrgl push origin refs/heads/main:main refs/heads/beta:beta",
			},
			{
				Comment: "push to main branch (destination ref is assumed to be the same as source ref)",
				Line:    "wrgl push origin refs/heads/main:",
			},
			{
				Comment: "push and set branch upstream",
				Line:    "wrgl push origin refs/heads/main:main --set-upstream",
			},
			{
				Comment: "remove branch main",
				Line:    "wrgl push origin :refs/heads/main",
			},
			{
				Comment: "force update branch (non-fast-forward)",
				Line:    "wrgl push origin +refs/heads/beta:beta",
			},
			{
				Comment: "push to my-repo reading from remote.my-repo.push",
				Line:    "wrgl push my-repo",
			},
			{
				Comment: "force update my-repo",
				Line:    "wrgl push my-repo --force",
			},
			{
				Comment: "turn the remote into a mirror of local repository",
				Line:    "wrgl push my-mirror-repo --mirror",
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
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()
			setUpstream, err := cmd.Flags().GetBool("set-upstream")
			if err != nil {
				return err
			}
			all, err := cmd.Flags().GetBool("all")
			if err != nil {
				return err
			}

			cs, err := credentials.NewStore()
			if err != nil {
				return err
			}
			logger := utils.GetLogger(cmd)
			clients := utils.NewClientMap(cs, *logger)

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
					branch := c.Branch[name]
					colorstring.Fprintf(cmd.OutOrStdout(), "pushing [bold]%s[reset]\n", name)
					if err := retryPushSingleRepo(
						cmd, c, db, rs, clients, []string{branch.Remote, fmt.Sprintf("refs/heads/%s:%s", name, branch.Merge)}, false, wrglDir,
					); err != nil {
						if apiclient.IsHTTPError(err, 404, "Not Found") {
							cmd.Println("Repository not found, skipping.")
						} else {
							return err
						}
					}
				}
				return nil
			}

			return retryPushSingleRepo(cmd, c, db, rs, clients, args, setUpstream, wrglDir)
		},
	}
	cmd.Flags().BoolP("force", "f", false, "force update remote branch in certain conditions.")
	cmd.Flags().BoolP("set-upstream", "u", false, strings.Join([]string{
		"for every branch that is up to date or successfully pushed, add upstream",
		"(tracking) reference, used by argument-less `wrgl pull`.",
	}, " "))
	cmd.Flags().Bool("mirror", false, strings.Join([]string{
		"instead of naming each ref to push, specifies that all refs (which includes",
		"but not limited to refs/heads/ and refs/remotes/) be mirrored to the remote",
		"repository. Newly created local refs will be pushed to the remote end, locally",
		"updated refs will be force updated on the remote end, and deleted refs will be",
		"removed from the remote end. This is the default if the configuration option",
		"remote.<remote>.mirror is set.",
	}, " "))
	cmd.Flags().Bool("all", false, "push all branches that have upstream configured.")
	cmd.Flags().Bool("no-progress", false, "don't display progress bar")
	return cmd
}

func setBranchUpstream(cmd *cobra.Command, wrglDir, remote string, refs []*conf.Refspec) error {
	s := conffs.NewStore(wrglDir, conffs.LocalSource, "")
	c, err := s.Open()
	if err != nil {
		return fmt.Errorf("error opening config store: %w", err)
	}
	if c.Branch == nil {
		c.Branch = map[string]*conf.Branch{}
	}
	for _, ref := range refs {
		if strings.HasPrefix(ref.Src(), "heads/") && strings.HasPrefix(ref.Dst(), "heads/") {
			branch := strings.TrimPrefix(ref.Src(), "heads/")
			merge := "refs/" + ref.Dst()
			c.Branch[branch] = &conf.Branch{
				Remote: remote,
				Merge:  merge,
			}
			// ensure that wrgl fetch pull merge
			rem := c.Remote[remote]
			sort.Sort(rem.Fetch)
			globRS := conf.MustParseRefspec(fmt.Sprintf("+refs/heads/*:refs/remotes/%s/*", remote))
			branchRS := conf.MustParseRefspec(
				fmt.Sprintf("+%s:refs/remotes/%s/%s", merge, remote, strings.TrimPrefix(ref.Dst(), "heads/")),
			)
			if rem.Fetch.IndexOf(globRS) == -1 && rem.Fetch.IndexOf(branchRS) == -1 {
				rem.Fetch = append(rem.Fetch, branchRS)
			}
			cmd.Printf("branch %q setup to track remote branch %q from %q\n", ref.Src()[6:], ref.Dst()[6:], remote)
		}
	}
	if err := s.Save(c); err != nil {
		return fmt.Errorf("error saving config: %w", err)
	}
	return nil
}

func getRepoToPush(c *conf.Config, args []string) (remote string, cr *conf.Remote, rem []string, err error) {
	if len(args) > 0 {
		if v, ok := c.Remote[args[0]]; ok {
			return args[0], v, args[1:], nil
		} else if v, ok := c.Remote["origin"]; ok {
			return "origin", v, args, nil
		} else {
			return "", nil, nil, fmt.Errorf("unrecognized repository name %q", args[0])
		}
	} else if v, ok := c.Remote["origin"]; ok {
		return "origin", v, args, nil
	}
	return "", nil, nil, fmt.Errorf("repository name not specified")
}

func getRefspecsToPush(cmd *cobra.Command, rs ref.Store, cr *conf.Remote, args []string, remoteRefs map[string][]byte, mirror bool) (refspecs []*conf.Refspec, err error) {
	if mirror {
		var localRefs map[string][]byte
		localRefs, err = ref.ListAllRefs(rs)
		if err != nil {
			return
		}
		for ref, sum := range localRefs {
			if v, ok := remoteRefs[ref]; !ok {
				refspecs = append(refspecs, conf.MustParseRefspec(fmt.Sprintf("refs/%s:refs/%s", ref, ref)))
			} else if !bytes.Equal(v, sum) {
				refspecs = append(refspecs, conf.MustParseRefspec(fmt.Sprintf("+refs/%s:refs/%s", ref, ref)))
			} else {
				fetch.DisplayRefUpdate(cmd, '=', "[up to date]", "", ref, ref)
			}
		}
		for ref := range remoteRefs {
			if _, ok := localRefs[ref]; !ok {
				refspecs = append(refspecs, conf.MustParseRefspec(fmt.Sprintf(":refs/%s", ref)))
			}
		}
		sort.Slice(refspecs, func(i, j int) bool {
			return refspecs[i].Dst() < refspecs[j].Dst()
		})
	} else if len(args) > 0 {
		for _, s := range args {
			rs, err := conf.ParseRefspec(s)
			if err != nil {
				return nil, err
			}
			if rs.Dst() == "" {
				found := false
				for _, obj := range cr.Push {
					if obj.Src() == rs.Src() {
						rs = obj
						found = true
						break
					}
				}
				if !found {
					rs, err = conf.NewRefspec(rs.Src(), rs.Src(), false, rs.Force)
					if err != nil {
						return nil, err
					}
				}
			}
			refspecs = append(refspecs, rs)
		}
	} else {
		refspecs = cr.Push
	}
	if len(refspecs) == 0 {
		return nil, fmt.Errorf("no refspec specified")
	}
	return
}

func interpretDestination(remoteRefs map[string][]byte, src, dst string) (string, error) {
	if strings.HasPrefix(dst, "refs/") {
		return strings.TrimPrefix(dst, "refs/"), nil
	}
	matchedRefs := []string{}
	for ref := range remoteRefs {
		if strings.HasSuffix(ref, "/"+dst) {
			matchedRefs = append(matchedRefs, ref)
		}
	}
	if len(matchedRefs) == 1 {
		return matchedRefs[0], nil
	} else if strings.HasPrefix(src, "refs/heads/") {
		if strings.HasPrefix(dst, "heads/") {
			return dst, nil
		} else {
			return "heads/" + dst, nil
		}
	} else if strings.HasPrefix(src, "refs/tags/") {
		if strings.HasPrefix(dst, "tags/") {
			return dst, nil
		} else {
			return "tags/" + dst, nil
		}
	}
	return "", fmt.Errorf("ambiguous push destination %q", dst)
}

type receivePackUpdate struct {
	Sum, OldSum      []byte
	Src, Dst, ErrMsg string
	Force            bool
}

func identifyUpdates(
	cmd *cobra.Command, db objects.Store, rs ref.Store, refspecs []*conf.Refspec, remoteRefs map[string][]byte, force bool,
) (upToDateRefspecs []*conf.Refspec, updates []*receivePackUpdate, err error) {
	for _, s := range refspecs {
		src := s.Src()
		dst := s.Dst()
		var sum []byte
		if src != "" {
			_, sum, _, err = ref.InterpretCommitName(db, rs, src, false)
			if err != nil {
				err = fmt.Errorf("error interpreting %q: %v", src, err)
				return
			}
		}
		dst, err = interpretDestination(remoteRefs, src, dst)
		if err != nil {
			return
		}
		if v, ok := remoteRefs[dst]; ok {
			if string(v) == string(sum) {
				fetch.DisplayRefUpdate(cmd, '=', "[up to date]", "", src, dst)
				upToDateRefspecs = append(upToDateRefspecs, s)
			} else if sum == nil {
				// delete ref
				updates = append(updates, &receivePackUpdate{
					OldSum: v,
					Sum:    sum,
					Src:    src,
					Dst:    dst,
				})
			} else if strings.HasPrefix(dst, "tags/") {
				if force || s.Force {
					updates = append(updates, &receivePackUpdate{
						OldSum: v,
						Sum:    sum,
						Src:    src,
						Dst:    dst,
						Force:  true,
					})
				} else {
					fetch.DisplayRefUpdate(cmd, '!', "[rejected]", "would clobber existing tag", src, dst)
				}
			} else if fastForward, err := ref.IsAncestorOf(db, v, sum); err != nil {
				return nil, nil, err
			} else if fastForward {
				updates = append(updates, &receivePackUpdate{
					OldSum: v,
					Sum:    sum,
					Src:    src,
					Dst:    dst,
				})
			} else if force || s.Force {
				updates = append(updates, &receivePackUpdate{
					OldSum: v,
					Sum:    sum,
					Src:    src,
					Dst:    dst,
					Force:  true,
				})
			} else {
				fetch.DisplayRefUpdate(cmd, '!', "[rejected]", "non-fast-forward", src, dst)
			}
		} else if sum != nil {
			updates = append(updates, &receivePackUpdate{
				OldSum: nil,
				Sum:    sum,
				Src:    src,
				Dst:    dst,
			})
		}
	}
	return
}

func reportUpdateStatus(cmd *cobra.Command, updates []*receivePackUpdate) {
	for _, u := range updates {
		if u.ErrMsg == "" {
			if u.Sum == nil {
				fetch.DisplayRefUpdate(cmd, '-', "[deleted]", "", "", u.Dst)
			} else if u.OldSum == nil {
				var summary string
				if strings.HasPrefix(u.Dst, "heads/") {
					summary = "[new branch]"
				} else if strings.HasPrefix(u.Dst, "tags/") {
					summary = "[new tag]"
				} else {
					summary = "[new reference]"
				}
				fetch.DisplayRefUpdate(cmd, '*', summary, "", u.Src, u.Dst)
			} else if u.Force {
				fetch.DisplayRefUpdate(cmd, '+', fetch.Quickref(u.OldSum, u.Sum, false), "forced update", u.Src, u.Dst)
			} else {
				fetch.DisplayRefUpdate(cmd, ' ', fetch.Quickref(u.OldSum, u.Sum, true), "", u.Src, u.Dst)
			}
		} else {
			fetch.DisplayRefUpdate(cmd, '!', "[remote rejected]", u.ErrMsg, u.Src, u.Dst)
		}
	}
}

// retryPushSingleRepo like pushSingleRepo but retry with exponential backoff
func retryPushSingleRepo(
	cmd *cobra.Command, c *conf.Config, db objects.Store, rs ref.Store, clients *utils.ClientMap,
	args []string, setUpstream bool, wrglDir string,
) error {
	logger := utils.GetLogger(cmd)
	ticker := backoff.NewTicker(backoff.NewExponentialBackOff())
	defer ticker.Stop()
	for range ticker.C {
		if err := pushSingleRepo(cmd, c, db, rs, clients, args, setUpstream, wrglDir); err != nil {
			var httpErr *apiclient.HTTPError
			if errors.As(err, &httpErr) && httpErr.Code >= 500 {
				logger.Error(err, "retrying push")
				if err := clients.ResetCookies(); err != nil {
					return err
				}
				continue
			}
			return err
		}
		break
	}
	return nil
}

func pushSingleRepo(
	cmd *cobra.Command, c *conf.Config, db objects.Store, rs ref.Store, clients *utils.ClientMap,
	args []string, setUpstream bool, wrglDir string,
) error {
	mirror, err := cmd.Flags().GetBool("mirror")
	if err != nil {
		return err
	}
	force, err := cmd.Flags().GetBool("force")
	if err != nil {
		return err
	}
	remote, cr, args, err := getRepoToPush(c, args)
	if err != nil {
		return err
	}
	if cr.Mirror {
		mirror = true
	}
	if mirror {
		force = true
	}
	client, err := clients.GetClient(cmd, cr.URL)
	if err != nil {
		return fmt.Errorf("error getting client: %w", err)
	}
	remoteRefs, err := clients.GetRefs(cmd, cr.URL)
	if err != nil {
		return fmt.Errorf("error getting remote refs: %w", err)
	}
	cmd.Printf("To %s\n", cr.URL)
	refspecs, err := getRefspecsToPush(cmd, rs, cr, args, remoteRefs, mirror)
	if err != nil {
		return fmt.Errorf("error getting refspecs to push: %w", err)
	}
	upToDateRefspecs, updates, err := identifyUpdates(cmd, db, rs, refspecs, remoteRefs, force)
	if err != nil {
		return fmt.Errorf("error identifying updates: %w", err)
	}
	if len(updates) > 0 {
		um := map[string]*payload.Update{}
		for _, u := range updates {
			um[u.Dst] = &payload.Update{
				Sum:    payload.BytesToHex(u.Sum),
				OldSum: payload.BytesToHex(u.OldSum),
			}
		}
		ses, err := apiclient.NewReceivePackSession(db, rs, client, um, remoteRefs, c.MaxPackFileSize())
		if err != nil {
			return utils.HandleHTTPError(cmd, clients.CredsStore, cr.URL, err)
		}
		if err := utils.WithProgressBar(cmd, false, func(cmd *cobra.Command, barContainer *pbar.Container) (err error) {
			um, err = ses.Start(barContainer)
			return err
		}); err != nil {
			return fmt.Errorf("error starting receive pack session: %w", err)
		}
		for _, u := range updates {
			if v, ok := um[u.Dst]; ok {
				u.ErrMsg = v.ErrMsg
			} else {
				u.ErrMsg = "remote failed to report status"
			}
		}
		reportUpdateStatus(cmd, updates)
		if username, reponame, ok := utils.IsWrglhubRemote(cr.URL); ok {
			cmd.Printf("See latest data at:\n")
			for _, u := range updates {
				if u.ErrMsg != "" || u.Sum == nil || (!strings.HasPrefix(u.Dst, "heads/") && !strings.HasPrefix(u.Dst, "refs/heads/")) {
					continue
				}
				branch := fetch.TrimRefPrefix(u.Dst)
				cmd.Printf("  %srefs/heads/%s\n", utils.RepoWebURI(username, reponame), branch)
			}
		}
	}
	if setUpstream {
		refs := []*conf.Refspec{}
		for _, rs := range upToDateRefspecs {
			ref, err := conf.NewRefspec(
				strings.TrimPrefix(rs.Src(), "refs/"),
				strings.TrimPrefix(rs.Dst(), "refs/"),
				false, false,
			)
			if err != nil {
				return fmt.Errorf("error creating up-to-date refspec: %w", err)
			}
			refs = append(refs, ref)
		}
		for _, u := range updates {
			ref, err := conf.NewRefspec(
				strings.TrimPrefix(u.Src, "refs/"),
				strings.TrimPrefix(u.Dst, "refs/"),
				false, false,
			)
			if err != nil {
				return fmt.Errorf("error creating updated refspec: %w", err)
			}
			refs = append(refs, ref)
		}
		return setBranchUpstream(cmd, wrglDir, remote, refs)
	}
	return nil
}
