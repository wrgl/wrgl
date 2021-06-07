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

	"github.com/mmcloughlin/meow"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	packclient "github.com/wrgl/core/pkg/pack/client"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/wrgl/utils"
)

func newFetchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fetch [REPOSITORY [REFSPEC...]]",
		Short: "Fetch branches and/or tags (collectively, \"refs\") from one or more other repositories, along with the objects necessary to complete their histories. Remote-tracking branches are updated.",
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
					err := fetch(cmd, db, fs, c.User, k, v, v.Fetch, force)
					if err != nil {
						return err
					}
				}
				return nil
			}

			var remote = "origin"
			if len(args) > 0 {
				remote = args[0]
			}
			rem := utils.MustGetRemote(cmd, c, remote)
			specs := rem.Fetch
			if len(args) > 1 {
				specs = make([]*versioning.Refspec, len(args)-1)
				for i, s := range args[1:] {
					rs, err := versioning.ParseRefspec(s)
					if err != nil {
						return err
					}
					specs[i] = rs
				}
			}
			return fetch(cmd, db, fs, c.User, remote, rem, specs, force)
		},
	}
	cmd.Flags().Bool("all", false, "Fetch all remotes.")
	cmd.Flags().BoolP("force", "f", false, "Force update local branch in certain conditions.")
	return cmd
}

func saveObjects(db kv.Store, fs kv.FileStore, wg *sync.WaitGroup, seed uint64, oc <-chan *packutils.Object, ec chan<- error) (commitsChan chan []byte) {
	commitsChan = make(chan []byte)
	go func() {
		defer close(commitsChan)
		for o := range oc {
			switch o.Type {
			case encoding.ObjectCommit:
				sum, err := versioning.SaveCommitBytes(db, seed, o.Content)
				if err != nil {
					ec <- err
					return
				}
				commitsChan <- sum
			case encoding.ObjectTable:
				tr, err := objects.NewTableReader(objects.NopCloser(bytes.NewReader(o.Content)))
				if err != nil {
					ec <- err
					return
				}
				b := table.NewBuilder(db, fs, tr.Columns, tr.PK, seed, 0)
				_, err = b.SaveTableBytes(o.Content, tr.RowsCount())
				if err != nil {
					ec <- err
					return
				}
			case encoding.ObjectRow:
				sum := meow.Checksum(seed, o.Content)
				err := table.SaveRow(db, sum[:], o.Content)
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

func identifyRefsToFetch(client *packclient.Client, specs []*versioning.Refspec) (refs []*Ref, dstRefs, maybeSaveTags map[string][]byte, advertised [][]byte, err error) {
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

func displayRefUpdate(cmd *cobra.Command, code byte, summary, errStr, from, to string) {
	if errStr != "" {
		errStr = fmt.Sprintf(" (%s)", errStr)
	}
	for _, prefix := range []string{
		"refs/heads/", "refs/tags/", "refs/remotes/",
	} {
		from = strings.TrimPrefix(from, prefix)
		to = strings.TrimPrefix(to, prefix)
	}
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
	cmd *cobra.Command, u *versioning.ConfigUser, db kv.Store, fs kv.FileStore, remoteURL string,
	fetchedCommits [][]byte, refs []*Ref, dstRefs, maybeSaveTags map[string][]byte, force bool,
) error {
	someFailed := false
	// if a remote tag point to an existing object then save that tag
	cm := bytesSliceToMap(fetchedCommits)
	for ref, sum := range maybeSaveTags {
		if _, ok := cm[string(sum)]; ok || versioning.CommitExist(db, sum) {
			_, err := versioning.GetRef(db, ref[5:])
			if err != nil {
				refs = append(refs, &Ref{ref, ref, false})
				dstRefs[ref] = sum
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
	for _, ref := range refs {
		oldSum, _ := versioning.GetRef(db, ref.Dst[5:])
		sum := dstRefs[ref.Dst]
		if oldSum != nil && strings.HasPrefix(ref.Dst, "refs/tags/") {
			if force || ref.Force {
				err := versioning.SaveRef(db, fs, ref.Dst[5:], sum, u.Name, u.Email, "fetch", "updating tag")
				if err != nil {
					displayRefUpdate(cmd, '!', "[tag update]", "unable to update local ref", ref.Src, ref.Dst)
					someFailed = true
				} else {
					displayRefUpdate(cmd, 't', "[tag update]", "", ref.Src, ref.Dst)
				}
			} else {
				displayRefUpdate(cmd, '!', "[rejected]", "would clobber existing tag", ref.Src, ref.Dst)
				someFailed = true
			}
			continue
		}
		if oldSum == nil {
			var msg, what string
			if strings.HasPrefix(ref.Src, "refs/tags/") {
				msg = "storing tag"
				what = "[new tag]"
			} else if strings.HasPrefix(ref.Src, "refs/heads") {
				msg = "storing head"
				what = "[new branch]"
			} else {
				msg = "storing ref"
				what = "[new ref]"
			}
			err := versioning.SaveRef(db, fs, ref.Dst[5:], sum, u.Name, u.Email, "fetch", msg)
			if err != nil {
				displayRefUpdate(cmd, '!', what, "unable to update local ref", ref.Src, ref.Dst)
				someFailed = true
			} else {
				displayRefUpdate(cmd, '*', what, "", ref.Src, ref.Dst)
			}
			continue
		}
		fastForward, err := versioning.IsAncestorOf(db, oldSum, sum)
		if err != nil {
			return err
		}
		if fastForward {
			err := versioning.SaveRef(db, fs, ref.Dst[5:], sum, u.Name, u.Email, "fetch", "fast-forward")
			qr := quickref(oldSum, sum, true)
			if err != nil {
				displayRefUpdate(cmd, '!', qr, "unable to update local ref", ref.Src, ref.Dst)
				someFailed = true
			} else {
				displayRefUpdate(cmd, ' ', qr, "", ref.Src, ref.Dst)
			}
		} else if force || ref.Force {
			err := versioning.SaveRef(db, fs, ref.Dst[5:], sum, u.Name, u.Email, "fetch", "forced-update")
			qr := quickref(oldSum, sum, false)
			if err != nil {
				displayRefUpdate(cmd, '!', qr, "unable to update local ref", ref.Src, ref.Dst)
				someFailed = true
			} else {
				displayRefUpdate(cmd, '+', qr, "forced update", ref.Src, ref.Dst)
			}
		} else {
			displayRefUpdate(cmd, '!', "[rejected]", "non-fast-forward", ref.Src, ref.Dst)
			someFailed = true
		}
	}
	if someFailed {
		return fmt.Errorf("failed to fetch some refs from " + remoteURL)
	}
	return nil
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

func fetchObjects(cmd *cobra.Command, db kv.Store, fs kv.FileStore, client *packclient.Client, advertised [][]byte) (fetchedCommits [][]byte, err error) {
	var wg sync.WaitGroup
	neg, err := packclient.NewNegotiator(db, fs, &wg, client, advertised, 0)
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
	cc := saveObjects(db, fs, &wg, 0, neg.ObjectChan, ec)
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

func fetch(cmd *cobra.Command, db kv.Store, fs kv.FileStore, u *versioning.ConfigUser, remote string, cr *versioning.ConfigRemote, specs []*versioning.Refspec, force bool) error {
	cmd.Printf("From %s\n", cr.URL)
	client, err := packclient.NewClient(db, fs, cr.URL)
	if err != nil {
		return err
	}
	refs, dstRefs, maybeSaveTags, advertised, err := identifyRefsToFetch(client, specs)
	if err != nil {
		return err
	}
	fetchedCommits, err := fetchObjects(cmd, db, fs, client, advertised)
	if err != nil {
		return err
	}
	return saveFetchedRefs(cmd, u, db, fs, cr.URL, fetchedCommits, refs, dstRefs, maybeSaveTags, force)
}
