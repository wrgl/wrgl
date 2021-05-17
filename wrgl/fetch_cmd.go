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
			dryRun, err := cmd.Flags().GetBool("dry-run")
			if err != nil {
				return err
			}
			force, err := cmd.Flags().GetBool("force")
			if err != nil {
				return err
			}
			if all {
				for k, v := range c.Remote {
					err := fetch(cmd, db, fs, c.User, k, v, v.Fetch, dryRun, force)
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
					rs, err := versioning.NewRefspec(s)
					if err != nil {
						return err
					}
					specs[i] = rs
				}
			}
			return fetch(cmd, db, fs, c.User, remote, rem, specs, dryRun, force)
		},
	}
	cmd.Flags().Bool("all", false, "Fetch all remotes.")
	cmd.Flags().Bool("dry-run", false, "Show what would be done, without making any changes.")
	cmd.Flags().BoolP("force", "f", false, "Force update local branch in certain conditions.")
	return cmd
}

func saveObjects(db kv.Store, fs kv.FileStore, wg *sync.WaitGroup, seed uint64, oc <-chan *packclient.Object, ec chan<- error) {
	for o := range oc {
		switch o.Type {
		case encoding.ObjectCommit:
			_, err := versioning.SaveCommitBytes(db, seed, o.Content)
			if err != nil {
				ec <- err
				return
			}
		case encoding.ObjectTable:
			tr, err := objects.NewTableReader(bytes.NewReader(o.Content))
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

func displayRefUpdate(cmd *cobra.Command, code byte, summary, errStr, remote, local string) {
	if errStr != "" {
		errStr = fmt.Sprintf(" (%s)", errStr)
	}
	for _, prefix := range []string{
		"refs/heads/", "refs/tags/",
	} {
		remote = strings.TrimPrefix(remote, prefix)
	}
	for _, prefix := range []string{
		"refs/heads/", "refs/tags/", "refs/remotes/",
	} {
		local = strings.TrimPrefix(local, prefix)
	}
	cmd.Printf(" %c %-19s %-11s -> %s%s\n", code, summary, remote, local, errStr)
}

func saveFetchedRefs(cmd *cobra.Command, u *versioning.ConfigUser, db kv.Store, fs kv.FileStore, remoteURL string, refs []*Ref, dstRefs, maybeSaveTags map[string][]byte, force bool) error {
	someFailed := false
	// if a remote tag point to an existing object then save that tag
	for ref, sum := range maybeSaveTags {
		if !versioning.CommitExist(db, sum) {
			continue
		}
		_, err := versioning.GetRef(db, ref[5:])
		if err != nil {
			refs = append(refs, &Ref{ref, ref, false})
			dstRefs[ref] = sum
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
			quickref := fmt.Sprintf("%s..%s", hex.EncodeToString(oldSum)[:7], hex.EncodeToString(sum)[:7])
			if err != nil {
				displayRefUpdate(cmd, '!', quickref, "unable to update local ref", ref.Src, ref.Dst)
				someFailed = true
			} else {
				displayRefUpdate(cmd, ' ', quickref, "", ref.Src, ref.Dst)
			}
		} else if force || ref.Force {
			err := versioning.SaveRef(db, fs, ref.Dst[5:], sum, u.Name, u.Email, "fetch", "forced-update")
			quickref := fmt.Sprintf("%s..%s", hex.EncodeToString(oldSum)[:7], hex.EncodeToString(sum)[:7])
			if err != nil {
				displayRefUpdate(cmd, '!', quickref, "unable to update local ref", ref.Src, ref.Dst)
				someFailed = true
			} else {
				displayRefUpdate(cmd, '+', quickref, "forced update", ref.Src, ref.Dst)
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

func fetchObjects(cmd *cobra.Command, db kv.Store, fs kv.FileStore, client *packclient.Client, advertised [][]byte, dryRun bool) (err error) {
	var wg sync.WaitGroup
	oc := make(chan *packclient.Object, 100)
	neg, err := packclient.NewNegotiator(db, fs, &wg, client, advertised, oc, 0)
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
	go saveObjects(db, fs, &wg, 0, oc, ec)
	err = neg.Start()
	if err != nil {
		return
	}
	wg.Wait()
	done <- true
	return
}

func fetch(cmd *cobra.Command, db kv.Store, fs kv.FileStore, u *versioning.ConfigUser, remote string, cr *versioning.ConfigRemote, specs []*versioning.Refspec, dryRun, force bool) error {
	cmd.Printf("From %s\n", cr.URL)
	client, err := packclient.NewClient(cr.URL)
	if err != nil {
		return err
	}
	refs, dstRefs, maybeSaveTags, advertised, err := identifyRefsToFetch(client, specs)
	if err != nil {
		return err
	}
	err = fetchObjects(cmd, db, fs, client, advertised, dryRun)
	if err != nil {
		return err
	}
	return saveFetchedRefs(cmd, u, db, fs, cr.URL, refs, dstRefs, maybeSaveTags, force)
}
