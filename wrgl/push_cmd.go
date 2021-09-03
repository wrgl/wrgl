// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	apiclient "github.com/wrgl/core/pkg/api/client"
	"github.com/wrgl/core/pkg/api/payload"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/local"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/wrgl/utils"
)

func newPushCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "push [REPOSITORY [REFSPEC...]]",
		Short: "Updates remote refs using local refs, while sending objects necessary to complete the given refs.",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			wrglDir := utils.MustWRGLDir(cmd)
			c, err := local.AggregateConfig(wrglDir)
			if err != nil {
				return err
			}
			rd := getRepoDir(cmd)
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

			remote, cr, args, err := getRepoToPush(c, args)
			if err != nil {
				return err
			}
			refspecs, err := getRefspecsToPush(cr, args)
			if err != nil {
				return err
			}
			client, err := apiclient.NewClient(cr.URL)
			if err != nil {
				return err
			}
			remoteRefs, err := client.GetRefs()
			if err != nil {
				return err
			}
			cmd.Printf("To %s\n", cr.URL)
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
				return err
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
	cmd.Flags().BoolP("set-upstream", "u", false, "for every branch that is up to date or successfully pushed, add upstream (tracking) reference, used by argument-less `wrgl pull`.")
	return cmd
}

func setBranchUpstream(cmd *cobra.Command, wrglDir, remote string, refs []*Ref) error {
	c, err := local.OpenConfig(false, false, wrglDir, "")
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
	return local.SaveConfig(c)
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

func getRefspecsToPush(cr *conf.Remote, args []string) (refspecs []*conf.Refspec, err error) {
	if len(args) > 0 {
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
