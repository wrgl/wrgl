// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package fetch

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
	"github.com/wrgl/wrgl/pkg/errors"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/pbar"
	"github.com/wrgl/wrgl/pkg/ref"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fetch [REPOSITORY [REFSPEC...]]",
		Short: "Download objects and refs from another repository.",
		Long:  "Fetch branches and/or tags (collectively, \"refs\") from another repository, along with the objects necessary to complete their histories. Remote-tracking branches are then updated.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "fetch the main branch from origin",
				Line:    "wrgl fetch origin refs/heads/main:refs/remotes/origin/main",
			},
			{
				Comment: "fetch multiple branches with glob pattern",
				Line:    "wrgl fetch origin refs/heads/*:refs/remotes/origin/*",
			},
			{
				Comment: "fetch from origin, reading refspec from remote.origin.fetch",
				Line:    "wrgl fetch",
			},
			{
				Comment: "fetch and force update a single branch",
				Line:    "wrgl fetch +refs/heads/main:refs/remotes/origin/main",
			},
			{
				Comment: "fetch and force all non-fast-forward updates",
				Line:    "wrgl fetch my-repo --force",
			},
			{
				Comment: "fetch the first 2 commits in full, fetch the rest shallowly",
				Line:    "wrgl fetch origin refs/heads/main:refs/remotes/origin/main --depth 2",
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
			all, err := cmd.Flags().GetBool("all")
			if err != nil {
				return err
			}
			force, err := cmd.Flags().GetBool("force")
			if err != nil {
				return err
			}
			depth, err := cmd.Flags().GetInt32("depth")
			if err != nil {
				return err
			}
			cs, err := credentials.NewStore()
			if err != nil {
				return err
			}
			cm, err := utils.NewClientMap()
			if err != nil {
				return err
			}
			return utils.WithProgressBar(cmd, false, func(cmd *cobra.Command, barContainer pbar.Container) error {
				if all {
					for k, v := range c.Remote {
						uri, tok, err := utils.GetCredentials(cmd, cs, v.URL)
						if err != nil {
							return err
						}
						err = Fetch(cmd, db, rs, cm, c.User, k, tok, v, v.Fetch, force, depth, logger, barContainer)
						if err != nil {
							return utils.HandleHTTPError(cmd, cs, v.URL, uri, err)
						}
					}
					return nil
				}
				remote, rem, specs, err := ParseRemoteAndRefspec(cmd, c, "", args)
				if err != nil {
					return err
				}
				uri, tok, err := utils.GetCredentials(cmd, cs, rem.URL)
				if err != nil {
					return err
				}
				if err := Fetch(cmd, db, rs, cm, c.User, remote, tok, rem, specs, force, depth, logger, barContainer); err != nil {
					return utils.HandleHTTPError(cmd, cs, rem.URL, uri, err)
				}
				return nil
			})
		},
	}
	cmd.Flags().Bool("all", false, "Fetch all remotes.")
	cmd.Flags().BoolP("force", "f", false, "Force update local branch in certain conditions.")
	cmd.Flags().Int32P("depth", "d", 0, "The maximum depth pass which commits will be fetched shallowly. Shallow commits only have the metadata but not the data itself. In other words, while you can still see the commit history, you cannot access its data. If depth is set to 0 then all missing commits will be fetched in full.")
	cmd.AddCommand(newTablesCmd())
	return cmd
}

func ParseRemoteAndRefspec(cmd *cobra.Command, c *conf.Config, branch string, args []string) (string, *conf.Remote, []*conf.Refspec, error) {
	var remote = "origin"
	b, ok := c.Branch[branch]
	if ok && b.Remote != "" {
		remote = b.Remote
	} else if len(args) > 0 {
		remote = args[0]
	}
	rem, ok := c.Remote[remote]
	if !ok {
		return "", nil, nil, fmt.Errorf("remote not found: %s", remote)
	}
	specs := rem.Fetch
	if len(args) > 1 {
		specs = make([]*conf.Refspec, len(args)-1)
		for i, s := range args[1:] {
			rs, err := conf.ParseRefspec(s)
			if err != nil {
				return "", nil, nil, err
			}
			specs[i] = rs
		}
	}
	return remote, rem, specs, nil
}

func identifyRefsToFetch(cmd *cobra.Command, cm *utils.ClientMap, cr *conf.Remote, specs []*conf.Refspec) (refs []*conf.Refspec, dstRefs, maybeSaveTags map[string][]byte, advertised [][]byte, err error) {
	m, err := cm.GetRefs(cmd, cr)
	if err != nil {
		return
	}
	dstRefs = map[string][]byte{}
	maybeSaveTags = map[string][]byte{}
	for r, sum := range m {
		covered := false
		for _, spec := range specs {
			dst := spec.DstForRef("refs/" + r)
			if dst != "" {
				dst = strings.TrimPrefix(dst, "refs/")
				dstRefs[dst] = sum
				advertised = append(advertised, sum)
				ref, err := conf.NewRefspec(r, dst, false, spec.Force)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				refs = append(refs, ref)
				covered = true
			}
		}
		if !covered && strings.HasPrefix(r, "tags/") {
			maybeSaveTags[r] = sum
		}
	}
	return
}

func TrimRefPrefix(r string) string {
	if strings.Contains(r, "/") &&
		!strings.HasPrefix(r, "refs/") &&
		!strings.HasPrefix(r, "heads/") &&
		!strings.HasPrefix(r, "tags/") &&
		!strings.HasPrefix(r, "remotes/") {
		// preserve "refs/" prefix for custom ref
		return "refs/" + r
	}
	for _, prefix := range []string{
		"refs/heads/", "refs/tags/", "refs/remotes/", "heads/", "tags/", "remotes/",
	} {
		r = strings.TrimPrefix(r, prefix)
	}
	return r
}

func DisplayRefUpdate(cmd *cobra.Command, code byte, summary, errStr, from, to string) {
	if errStr != "" {
		errStr = fmt.Sprintf(" (%s)", errStr)
	}
	from = TrimRefPrefix(from)
	to = TrimRefPrefix(to)
	cmd.Printf(" %c %-17s %-11s -> %s%s\n", code, summary, from, to, errStr)
}

func bytesSliceToMap(sl [][]byte) (m map[string]struct{}) {
	m = make(map[string]struct{})
	for _, b := range sl {
		m[string(b)] = struct{}{}
	}
	return m
}

func Quickref(oldSum, sum []byte, fastForward bool) string {
	a := hex.EncodeToString(oldSum)[:7]
	b := hex.EncodeToString(sum)[:7]
	if fastForward {
		return fmt.Sprintf("%s..%s", a, b)
	}
	return fmt.Sprintf("%s...%s", a, b)
}

func saveFetchedRefs(
	cmd *cobra.Command, u *conf.User, db objects.Store, rs ref.Store, remoteName, remoteURL string,
	fetchedCommits [][]byte, refs []*conf.Refspec, dstRefs, maybeSaveTags map[string][]byte, force bool,
) ([]*conf.Refspec, error) {
	someFailed := false
	// if a remote tag point to an existing object then save that tag
	cm := bytesSliceToMap(fetchedCommits)
	for r, sum := range maybeSaveTags {
		if _, ok := cm[string(sum)]; ok || objects.CommitExist(db, sum) {
			_, err := ref.GetRef(rs, r)
			if err != nil {
				ref, err := conf.NewRefspec(r, r, false, false)
				if err != nil {
					return nil, err
				}
				refs = append(refs, ref)
				dstRefs[r] = sum
			}
		}
	}
	// sort refs so that output is always deterministic
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Src() < refs[j].Src() {
			return true
		} else if refs[i].Src() > refs[j].Src() {
			return false
		}
		return refs[i].Dst() < refs[j].Dst()
	})
	savedRefs := []*conf.Refspec{}
	remoteDisplayed := false
	for _, r := range refs {
		oldSum, _ := ref.GetRef(rs, r.Dst())
		sum := dstRefs[r.Dst()]
		if bytes.Equal(oldSum, sum) {
			continue
		}
		if !remoteDisplayed {
			cmd.Printf("From %s\n", remoteURL)
			remoteDisplayed = true
		}
		if oldSum != nil && strings.HasPrefix(r.Dst(), "tags/") {
			if force || r.Force {
				err := ref.SaveFetchRef(rs, r.Dst(), sum, u.Name, u.Email, remoteName, "updating tag")
				if err != nil {
					DisplayRefUpdate(cmd, '!', "[tag update]", "unable to update local ref", r.Src(), r.Dst())
					someFailed = true
				} else {
					DisplayRefUpdate(cmd, 't', "[tag update]", "", r.Src(), r.Dst())
					savedRefs = append(savedRefs, r)
				}
			} else {
				DisplayRefUpdate(cmd, '!', "[rejected]", "would clobber existing tag", r.Src(), r.Dst())
				someFailed = true
			}
			continue
		}
		if oldSum == nil {
			var msg, what string
			if strings.HasPrefix(r.Src(), "tags/") {
				msg = "storing tag"
				what = "[new tag]"
			} else if strings.HasPrefix(r.Src(), "heads/") {
				msg = "storing head"
				what = "[new branch]"
			} else {
				msg = "storing ref"
				what = "[new ref]"
			}
			err := ref.SaveFetchRef(rs, r.Dst(), sum, u.Name, u.Email, remoteName, msg)
			if err != nil {
				DisplayRefUpdate(cmd, '!', what, "unable to update local ref", r.Src(), r.Dst())
				someFailed = true
			} else {
				DisplayRefUpdate(cmd, '*', what, "", r.Src(), r.Dst())
				savedRefs = append(savedRefs, r)
			}
			continue
		}
		fastForward, err := ref.IsAncestorOf(db, oldSum, sum)
		if err != nil {
			return nil, err
		}
		if fastForward {
			err := ref.SaveFetchRef(rs, r.Dst(), sum, u.Name, u.Email, remoteName, "fast-forward")
			qr := Quickref(oldSum, sum, true)
			if err != nil {
				DisplayRefUpdate(cmd, '!', qr, "unable to update local ref", r.Src(), r.Dst())
				someFailed = true
			} else {
				DisplayRefUpdate(cmd, ' ', qr, "", r.Src(), r.Dst())
				savedRefs = append(savedRefs, r)
			}
		} else if force || r.Force {
			err := ref.SaveFetchRef(rs, r.Dst(), sum, u.Name, u.Email, remoteName, "forced-update")
			qr := Quickref(oldSum, sum, false)
			if err != nil {
				DisplayRefUpdate(cmd, '!', qr, "unable to update local ref", r.Src(), r.Dst())
				someFailed = true
			} else {
				DisplayRefUpdate(cmd, '+', qr, "forced update", r.Src(), r.Dst())
				savedRefs = append(savedRefs, r)
			}
		} else {
			DisplayRefUpdate(cmd, '!', "[rejected]", "non-fast-forward", r.Src(), r.Dst())
			someFailed = true
		}
	}
	if someFailed {
		return nil, fmt.Errorf("failed to fetch some refs from " + remoteURL)
	}
	return savedRefs, nil
}

func fetchObjects(
	cmd *cobra.Command,
	db objects.Store,
	rs ref.Store,
	client *apiclient.Client,
	advertised [][]byte,
	depth int32,
	container pbar.Container,
) (fetchedCommits [][]byte, err error) {
	bar := container.NewBar(-1, "Fetching objects", 0)
	defer bar.Abort()
	ses, err := apiclient.NewUploadPackSession(db, rs, client, advertised,
		apiclient.WithUploadPackDepth(int(depth)),
		apiclient.WithUploadPackProgressBar(bar),
	)
	if err != nil {
		if err.Error() == "nothing wanted" {
			err = nil
			return
		}
		err = errors.Wrap("error creating new upload pack session", err)
		return
	}
	bar.Done()
	return ses.Start()
}

func Fetch(
	cmd *cobra.Command,
	db objects.Store,
	rs ref.Store,
	cm *utils.ClientMap,
	u *conf.User,
	remote,
	token string,
	cr *conf.Remote,
	specs []*conf.Refspec,
	force bool,
	depth int32,
	logger *logr.Logger,
	container pbar.Container,
) error {
	client, err := apiclient.NewClient(cr.URL, apiclient.WithAuthorization(token), apiclient.WithLogger(logger))
	if err != nil {
		return errors.Wrap("error creating new client", err)
	}
	refs, dstRefs, maybeSaveTags, advertised, err := identifyRefsToFetch(cmd, cm, cr, specs)
	if err != nil {
		return errors.Wrap("error fetching refs", err)
	}
	fetchedCommits, err := fetchObjects(cmd, db, rs, client, advertised, depth, container)
	if err != nil {
		return errors.Wrap("error fetching objects", err)
	}
	_, err = saveFetchedRefs(cmd, u, db, rs, remote, cr.URL, fetchedCommits, refs, dstRefs, maybeSaveTags, force)
	return err
}
