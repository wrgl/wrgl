package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	packclient "github.com/wrgl/core/pkg/pack/client"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/wrgl/utils"
)

func newPushCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "push [REPOSITORY [REFSPEC...]]",
		Short: "Updates remote refs using local refs, while sending objects necessary to complete the given refs.",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			wrglDir := utils.MustWRGLDir(cmd)
			c, err := versioning.AggregateConfig(wrglDir)
			if err != nil {
				return err
			}
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

			cr, args, err := getRepoToPush(c, args)
			if err != nil {
				return err
			}

			refspecs, err := getRefspecsToPush(cr, args)
			if err != nil {
				return err
			}

			client, err := packclient.NewClient(db, fs, cr.URL)
			if err != nil {
				return err
			}
			remoteRefs, err := client.GetRefsInfo()
			if err != nil {
				return err
			}
			updates, commits, err := identifyUpdates(cmd, db, refspecs, remoteRefs, force)
			if err != nil {
				return err
			}
			commonCommits := identifyCommonCommits(db, remoteRefs)
			err = client.PostReceivePack(updates, commits, commonCommits)
			if err != nil {
				return err
			}
			reportUpdateStatus(cmd, updates)
			return nil
		},
	}
	cmd.Flags().BoolP("force", "f", false, "Force update remote branch in certain conditions.")
	return cmd
}

func identifyCommonCommits(db kv.DB, remoteRefs map[string][]byte) [][]byte {
	m := map[string]struct{}{}
	sums := [][]byte{}
	for _, v := range remoteRefs {
		if _, ok := m[string(v)]; ok {
			continue
		} else {
			m[string(v)] = struct{}{}
		}
		if versioning.CommitExist(db, v) {
			sums = append(sums, v)
		}
	}
	return sums
}

func getRepoToPush(c *versioning.Config, args []string) (cr *versioning.ConfigRemote, rem []string, err error) {
	if len(args) > 0 {
		if v, ok := c.Remote[args[0]]; ok {
			return v, args[1:], nil
		} else if v, ok := c.Remote["origin"]; ok {
			return v, args, nil
		} else {
			return nil, nil, fmt.Errorf("unrecognized repository name %q", args[0])
		}
	} else if v, ok := c.Remote["origin"]; ok {
		return v, args, nil
	}
	return nil, nil, fmt.Errorf("repository name not specified")
}

func getRefspecsToPush(cr *versioning.ConfigRemote, args []string) (refspecs []*versioning.Refspec, err error) {
	if len(args) > 0 {
		for _, s := range args {
			rs, err := versioning.ParseRefspec(s)
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
					rs, err = versioning.NewRefspec(rs.Src(), rs.Src(), false, rs.Force)
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
		return dst, nil
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
			return "refs/" + dst, nil
		} else {
			return "refs/heads/" + dst, nil
		}
	} else if strings.HasPrefix(src, "refs/tags/") {
		if strings.HasPrefix(dst, "tags/") {
			return "refs/" + dst, nil
		} else {
			return "refs/tags/" + dst, nil
		}
	}
	return "", fmt.Errorf("ambiguous push destination %q", dst)
}

func identifyUpdates(
	cmd *cobra.Command, db kv.DB, refspecs []*versioning.Refspec, remoteRefs map[string][]byte, force bool,
) (updates []*packutils.Update, commitsToSend []*objects.Commit, err error) {
	for _, rs := range refspecs {
		src := rs.Src()
		dst := rs.Dst()
		var sum []byte
		var commit *objects.Commit
		if src != "" {
			_, sum, commit, err = versioning.InterpretCommitName(db, src, false)
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
			} else if sum == nil {
				// delete ref
				updates = append(updates, &packutils.Update{
					OldSum: v,
					Sum:    sum,
					Src:    src,
					Dst:    dst,
				})
			} else if strings.HasPrefix(dst, "refs/tags/") {
				if force || rs.Force {
					updates = append(updates, &packutils.Update{
						OldSum: v,
						Sum:    sum,
						Src:    src,
						Dst:    dst,
						Force:  true,
					})
					commitsToSend = append(commitsToSend, commit)
				} else {
					displayRefUpdate(cmd, '!', "[rejected]", "would clobber existing tag", src, dst)
				}
			} else if fastForward, err := versioning.IsAncestorOf(db, v, sum); err != nil {
				return nil, nil, err
			} else if fastForward {
				updates = append(updates, &packutils.Update{
					OldSum: v,
					Sum:    sum,
					Src:    src,
					Dst:    dst,
				})
				commitsToSend = append(commitsToSend, commit)
			} else if force || rs.Force {
				updates = append(updates, &packutils.Update{
					OldSum: v,
					Sum:    sum,
					Src:    src,
					Dst:    dst,
					Force:  true,
				})
				commitsToSend = append(commitsToSend, commit)
			} else {
				displayRefUpdate(cmd, '!', "[rejected]", "non-fast-forward", src, dst)
			}
		} else if sum != nil {
			updates = append(updates, &packutils.Update{
				OldSum: nil,
				Sum:    sum,
				Src:    src,
				Dst:    dst,
			})
		}
	}
	return
}

func reportUpdateStatus(cmd *cobra.Command, updates []*packutils.Update) {
	for _, u := range updates {
		if u.ErrMsg == "" {
			if u.Sum == nil {
				displayRefUpdate(cmd, '-', "[deleted]", "", "", u.Dst)
			} else if u.OldSum == nil {
				var summary string
				if strings.HasPrefix(u.Dst, "refs/heads/") {
					summary = "[new branch]"
				} else if strings.HasPrefix(u.Dst, "refs/tags/") {
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
