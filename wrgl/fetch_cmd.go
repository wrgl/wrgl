// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/objects"
	packclient "github.com/wrgl/core/pkg/pack/client"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/wrgl/utils"
)

func newFetchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fetch [REPOSITORY [REFSPEC...]]",
		Short: "Fetch branches and/or tags (collectively, \"refs\") from one or more other repositories, along with the objects necessary to complete their histories. Remote-tracking branches are updated.",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			wrglDir := utils.MustWRGLDir(cmd)
			c, err := utils.AggregateConfig(wrglDir)
			if err != nil {
				return err
			}
			ensureUserSet(cmd, c)
			rd := getRepoDir(cmd)
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()
			all, err := cmd.Flags().GetBool("all")
			if err != nil {
				return err
			}
			force, err := cmd.Flags().GetBool("force")
			if err != nil {
				return err
			}
			if all {
				for k, v := range c.Remote {
					err := fetch(cmd, db, rs, c.User, k, v, v.Fetch, force)
					if err != nil {
						return err
					}
				}
				return nil
			}
			remote, rem, specs, err := parseRemoteAndRefspec(cmd, c, args)
			if err != nil {
				return err
			}
			return fetch(cmd, db, rs, c.User, remote, rem, specs, force)
		},
	}
	cmd.Flags().Bool("all", false, "Fetch all remotes.")
	cmd.Flags().BoolP("force", "f", false, "Force update local branch in certain conditions.")
	return cmd
}

func parseRemoteAndRefspec(cmd *cobra.Command, c *conf.Config, args []string) (string, *conf.ConfigRemote, []*conf.Refspec, error) {
	var remote = "origin"
	if len(args) > 0 {
		remote = args[0]
	}
	rem := utils.MustGetRemote(cmd, c, remote)
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

func saveObjects(db objects.Store, rs ref.Store, wg *sync.WaitGroup, oc <-chan *packutils.Object, ec chan<- error) (commitsChan chan []byte) {
	commitsChan = make(chan []byte)
	go func() {
		defer close(commitsChan)
		for o := range oc {
			switch o.Type {
			case encoding.ObjectCommit:
				sum, err := objects.SaveCommit(db, o.Content)
				if err != nil {
					ec <- err
					return
				}
				commitsChan <- sum
			case encoding.ObjectTable:
				_, err := objects.SaveTable(db, o.Content)
				if err != nil {
					ec <- err
					return
				}
			case encoding.ObjectBlock:
				_, err := objects.SaveBlock(db, o.Content)
				if err != nil {
					ec <- err
					return
				}
			}
			wg.Done()
		}
	}()
	return commitsChan
}

func exitOnError(cmd *cobra.Command, done <-chan bool, ec <-chan error) {
	select {
	case err := <-ec:
		cmd.PrintErrln(err.Error())
		os.Exit(1)
	case <-done:
		return
	}
}

type Ref struct {
	Src   string
	Dst   string
	Force bool
}

func identifyRefsToFetch(client *packclient.Client, specs []*conf.Refspec) (refs []*Ref, dstRefs, maybeSaveTags map[string][]byte, advertised [][]byte, err error) {
	m, err := client.GetRefsInfo()
	if err != nil {
		return
	}
	dstRefs = map[string][]byte{}
	maybeSaveTags = map[string][]byte{}
	for ref, sum := range m {
		covered := false
		for _, spec := range specs {
			dst := spec.DstForRef(ref)
			if dst != "" {
				dstRefs[dst] = sum
				advertised = append(advertised, sum)
				refs = append(refs, &Ref{
					ref, dst, spec.Force,
				})
				covered = true
			}
		}
		if !covered && strings.HasPrefix(ref, "refs/tags/") {
			maybeSaveTags[ref] = sum
		}
	}
	return
}

func trimRefPrefix(ref string) string {
	for _, prefix := range []string{
		"refs/heads/", "refs/tags/", "refs/remotes/",
	} {
		ref = strings.TrimPrefix(ref, prefix)
	}
	return ref
}

func displayRefUpdate(cmd *cobra.Command, code byte, summary, errStr, from, to string) {
	if errStr != "" {
		errStr = fmt.Sprintf(" (%s)", errStr)
	}
	from = trimRefPrefix(from)
	to = trimRefPrefix(to)
	cmd.Printf(" %c %-17s %-11s -> %s%s\n", code, summary, from, to, errStr)
}

func bytesSliceToMap(sl [][]byte) (m map[string]struct{}) {
	m = make(map[string]struct{})
	for _, b := range sl {
		m[string(b)] = struct{}{}
	}
	return m
}

func quickref(oldSum, sum []byte, fastForward bool) string {
	a := hex.EncodeToString(oldSum)[:7]
	b := hex.EncodeToString(sum)[:7]
	if fastForward {
		return fmt.Sprintf("%s..%s", a, b)
	}
	return fmt.Sprintf("%s...%s", a, b)
}

func saveFetchedRefs(
	cmd *cobra.Command, u *conf.ConfigUser, db objects.Store, rs ref.Store, remoteURL string,
	fetchedCommits [][]byte, refs []*Ref, dstRefs, maybeSaveTags map[string][]byte, force bool,
) ([]*Ref, error) {
	someFailed := false
	// if a remote tag point to an existing object then save that tag
	cm := bytesSliceToMap(fetchedCommits)
	for r, sum := range maybeSaveTags {
		if _, ok := cm[string(sum)]; ok || objects.CommitExist(db, sum) {
			_, err := ref.GetRef(rs, r[5:])
			if err != nil {
				refs = append(refs, &Ref{r, r, false})
				dstRefs[r] = sum
			}
		}
	}
	// sort refs so that output is always deterministic
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Src < refs[j].Src {
			return true
		} else if refs[i].Src > refs[j].Src {
			return false
		}
		return refs[i].Dst < refs[j].Dst
	})
	savedRefs := []*Ref{}
	remoteDisplayed := false
	for _, r := range refs {
		oldSum, _ := ref.GetRef(rs, r.Dst[5:])
		sum := dstRefs[r.Dst]
		if bytes.Equal(oldSum, sum) {
			continue
		}
		if !remoteDisplayed {
			cmd.Printf("From %s\n", remoteURL)
			remoteDisplayed = true
		}
		if oldSum != nil && strings.HasPrefix(r.Dst, "refs/tags/") {
			if force || r.Force {
				err := ref.SaveRef(rs, r.Dst[5:], sum, u.Name, u.Email, "fetch", "updating tag")
				if err != nil {
					displayRefUpdate(cmd, '!', "[tag update]", "unable to update local ref", r.Src, r.Dst)
					someFailed = true
				} else {
					displayRefUpdate(cmd, 't', "[tag update]", "", r.Src, r.Dst)
					savedRefs = append(savedRefs, r)
				}
			} else {
				displayRefUpdate(cmd, '!', "[rejected]", "would clobber existing tag", r.Src, r.Dst)
				someFailed = true
			}
			continue
		}
		if oldSum == nil {
			var msg, what string
			if strings.HasPrefix(r.Src, "refs/tags/") {
				msg = "storing tag"
				what = "[new tag]"
			} else if strings.HasPrefix(r.Src, "refs/heads") {
				msg = "storing head"
				what = "[new branch]"
			} else {
				msg = "storing ref"
				what = "[new ref]"
			}
			err := ref.SaveRef(rs, r.Dst[5:], sum, u.Name, u.Email, "fetch", msg)
			if err != nil {
				displayRefUpdate(cmd, '!', what, "unable to update local ref", r.Src, r.Dst)
				someFailed = true
			} else {
				displayRefUpdate(cmd, '*', what, "", r.Src, r.Dst)
				savedRefs = append(savedRefs, r)
			}
			continue
		}
		fastForward, err := ref.IsAncestorOf(db, oldSum, sum)
		if err != nil {
			return nil, err
		}
		if fastForward {
			err := ref.SaveRef(rs, r.Dst[5:], sum, u.Name, u.Email, "fetch", "fast-forward")
			qr := quickref(oldSum, sum, true)
			if err != nil {
				displayRefUpdate(cmd, '!', qr, "unable to update local ref", r.Src, r.Dst)
				someFailed = true
			} else {
				displayRefUpdate(cmd, ' ', qr, "", r.Src, r.Dst)
				savedRefs = append(savedRefs, r)
			}
		} else if force || r.Force {
			err := ref.SaveRef(rs, r.Dst[5:], sum, u.Name, u.Email, "fetch", "forced-update")
			qr := quickref(oldSum, sum, false)
			if err != nil {
				displayRefUpdate(cmd, '!', qr, "unable to update local ref", r.Src, r.Dst)
				someFailed = true
			} else {
				displayRefUpdate(cmd, '+', qr, "forced update", r.Src, r.Dst)
				savedRefs = append(savedRefs, r)
			}
		} else {
			displayRefUpdate(cmd, '!', "[rejected]", "non-fast-forward", r.Src, r.Dst)
			someFailed = true
		}
	}
	if someFailed {
		return nil, fmt.Errorf("failed to fetch some refs from " + remoteURL)
	}
	return savedRefs, nil
}

func collectCommitSums(commitsChan <-chan []byte) (commits *[][]byte, done chan bool) {
	done = make(chan bool)
	commits = &[][]byte{}
	go func() {
		defer close(done)
		for commit := range commitsChan {
			*commits = append(*commits, commit)
		}
		done <- true
	}()
	return
}

func fetchObjects(cmd *cobra.Command, db objects.Store, rs ref.Store, client *packclient.Client, advertised [][]byte) (fetchedCommits [][]byte, err error) {
	var wg sync.WaitGroup
	neg, err := packclient.NewNegotiator(db, rs, &wg, client, advertised, 0)
	if err != nil {
		if err.Error() == "nothing wanted" {
			err = nil
			return
		}
		return
	}
	done := make(chan bool)
	ec := make(chan error)
	go exitOnError(cmd, done, ec)
	cc := saveObjects(db, rs, &wg, neg.ObjectChan, ec)
	sums, collectDone := collectCommitSums(cc)
	err = neg.Start()
	if err != nil {
		return
	}
	wg.Wait()
	<-collectDone
	done <- true
	return *sums, nil
}

func fetch(cmd *cobra.Command, db objects.Store, rs ref.Store, u *conf.ConfigUser, remote string, cr *conf.ConfigRemote, specs []*conf.Refspec, force bool) error {
	client, err := packclient.NewClient(db, cr.URL)
	if err != nil {
		return err
	}
	refs, dstRefs, maybeSaveTags, advertised, err := identifyRefsToFetch(client, specs)
	if err != nil {
		return err
	}
	fetchedCommits, err := fetchObjects(cmd, db, rs, client, advertised)
	if err != nil {
		return err
	}
	_, err = saveFetchedRefs(cmd, u, db, rs, cr.URL, fetchedCommits, refs, dstRefs, maybeSaveTags, force)
	return err
}
