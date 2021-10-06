// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/credentials"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func newPushCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "push [REPOSITORY [REFSPEC...]]",
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
			force, err := cmd.Flags().GetBool("force")
			if err != nil {
				return err
			}
			setUpstream, err := cmd.Flags().GetBool("set-upstream")
			if err != nil {
				return err
			}
			mirror, err := cmd.Flags().GetBool("mirror")
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
			cs, err := credentials.NewStore()
			if err != nil {
				return err
			}
			uri, tok, err := getCredentials(cmd, cs, cr.URL)
			if err != nil {
				return err
			}
			client, err := apiclient.NewClient(cr.URL, apiclient.WithAuthorization(tok))
			if err != nil {
				return err
			}
			remoteRefs, err := client.GetRefs()
			if err != nil {
				return handleHTTPError(cmd, cs, *uri, err)
			}
			cmd.Printf("To %s\n", cr.URL)
			refspecs, err := getRefspecsToPush(cmd, rs, cr, args, remoteRefs, mirror)
			if err != nil {
				return err
			}
			upToDateRefspecs, updates, err := identifyUpdates(cmd, db, rs, refspecs, remoteRefs, force)
			if err != nil {
				return err
			}
			um := map[string]*payload.Update{}
			for _, u := range updates {
				um[u.Dst] = &payload.Update{
					Sum:    payload.BytesToHex(u.Sum),
					OldSum: payload.BytesToHex(u.OldSum),
				}
			}
			ses, err := apiclient.NewReceivePackSession(db, rs, client, um, remoteRefs, 0)
			if err != nil {
				return handleHTTPError(cmd, cs, *uri, err)
			}
			um, err = ses.Start()
			if err != nil {
				return err
			}
			for _, u := range updates {
				if v, ok := um[u.Dst]; ok {
					u.ErrMsg = v.ErrMsg
				} else {
					u.ErrMsg = "remote failed to report status"
				}
			}
			reportUpdateStatus(cmd, updates)
			if setUpstream {
				refs := []*Ref{}
				for _, rs := range upToDateRefspecs {
					refs = append(refs, &Ref{
						Src: strings.TrimPrefix(rs.Src(), "refs/"),
						Dst: strings.TrimPrefix(rs.Dst(), "refs/"),
					})
				}
				for _, u := range updates {
					refs = append(refs, &Ref{
						Src: strings.TrimPrefix(u.Src, "refs/"),
						Dst: strings.TrimPrefix(u.Dst, "refs/"),
					})
				}
				return setBranchUpstream(cmd, wrglDir, remote, refs)
			}
			return nil
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
	return cmd
}

func setBranchUpstream(cmd *cobra.Command, wrglDir, remote string, refs []*Ref) error {
	s := conffs.NewStore(wrglDir, conffs.LocalSource, "")
	c, err := s.Open()
	if err != nil {
		return err
	}
	if c.Branch == nil {
		c.Branch = map[string]*conf.Branch{}
	}
	for _, ref := range refs {
		if strings.HasPrefix(ref.Src, "heads/") && strings.HasPrefix(ref.Dst, "heads/") {
			c.Branch[ref.Src[6:]] = &conf.Branch{
				Remote: remote,
				Merge:  "refs/" + ref.Dst,
			}
			cmd.Printf("branch %q setup to track remote branch %q from %q\n", ref.Src[6:], ref.Dst[6:], remote)
		}
	}
	return s.Save(c)
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
				displayRefUpdate(cmd, '=', "[up to date]", "", ref, ref)
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
				err = fmt.Errorf("unrecognized ref %q", src)
				return
			}
		}
		dst, err = interpretDestination(remoteRefs, src, dst)
		if err != nil {
			return
		}
		if v, ok := remoteRefs[dst]; ok {
			if string(v) == string(sum) {
				displayRefUpdate(cmd, '=', "[up to date]", "", src, dst)
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
					displayRefUpdate(cmd, '!', "[rejected]", "would clobber existing tag", src, dst)
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
				displayRefUpdate(cmd, '!', "[rejected]", "non-fast-forward", src, dst)
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
				displayRefUpdate(cmd, '-', "[deleted]", "", "", u.Dst)
			} else if u.OldSum == nil {
				var summary string
				if strings.HasPrefix(u.Dst, "heads/") {
					summary = "[new branch]"
				} else if strings.HasPrefix(u.Dst, "tags/") {
					summary = "[new tag]"
				} else {
					summary = "[new reference]"
				}
				displayRefUpdate(cmd, '*', summary, "", u.Src, u.Dst)
			} else if u.Force {
				displayRefUpdate(cmd, '+', quickref(u.OldSum, u.Sum, false), "forced update", u.Src, u.Dst)
			} else {
				displayRefUpdate(cmd, ' ', quickref(u.OldSum, u.Sum, true), "", u.Src, u.Dst)
			}
		} else {
			displayRefUpdate(cmd, '!', "[remote rejected]", u.ErrMsg, u.Src, u.Dst)
		}
	}
}
