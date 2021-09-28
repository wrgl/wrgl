// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package wrgl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/core/cmd/wrgl/utils"
	apiclient "github.com/wrgl/core/pkg/api/client"
	"github.com/wrgl/core/pkg/conf"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	"github.com/wrgl/core/pkg/credentials"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

func newFetchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fetch [REPOSITORY [REFSPEC...]]",
		Short: "Download objects and refs from another repository",
		Long:  "Fetch branches and/or tags (collectively, \"refs\") from one or another repository, along with the objects necessary to complete their histories. Remote-tracking branches are updated.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "fetch from origin according to remote.origin.fetch",
				Line:    "wrgl fetch",
			},
			{
				Comment: "fetch from another named repository",
				Line:    "wrgl fetch my-repo",
			},
			{
				Comment: "fetch only the main branch from origin",
				Line:    "wrgl fetch origin refs/heads/main:refs/remotes/origin/main",
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
			cs, err := credentials.NewStore()
			if err != nil {
				return err
			}
			if all {
				for k, v := range c.Remote {
					uri, tok, err := getCredentials(cmd, cs, v.URL)
					if err != nil {
						return err
					}
					err = fetch(cmd, db, rs, c.User, k, tok, v, v.Fetch, force)
					if err != nil {
						return handleHTTPError(cmd, cs, *uri, err)
					}
				}
				return nil
			}
			remote, rem, specs, err := parseRemoteAndRefspec(cmd, c, args)
			if err != nil {
				return err
			}
			uri, tok, err := getCredentials(cmd, cs, rem.URL)
			if err != nil {
				return err
			}
			if err := fetch(cmd, db, rs, c.User, remote, tok, rem, specs, force); err != nil {
				return handleHTTPError(cmd, cs, *uri, err)
			}
			return nil
		},
	}
	cmd.Flags().Bool("all", false, "Fetch all remotes.")
	cmd.Flags().BoolP("force", "f", false, "Force update local branch in certain conditions.")
	return cmd
}

func handleHTTPError(cmd *cobra.Command, cs *credentials.Store, uri url.URL, err error) error {
	if v, ok := err.(*apiclient.HTTPError); ok && v.Code == http.StatusUnauthorized {
		cmd.PrintErrf("Credentials are invalid: %s\n", v.Error())
		if err := discardCredentials(cmd, cs, uri); err != nil {
			return err
		}
		os.Exit(1)
	}
	return err
}

func getCredentials(cmd *cobra.Command, cs *credentials.Store, remote string) (uri *url.URL, token string, err error) {
	u, err := url.Parse(remote)
	if err != nil {
		return
	}
	uri, token = cs.GetTokenMatching(*u)
	if uri == nil {
		cmd.Printf("No credential found for %s\n", remote)
		cmd.Printf("Run this command to authenticate:\n    wrgl credentials authenticate %s\n", remote)
		err = fmt.Errorf("unauthorized")
		return
	}
	return
}

func discardCredentials(cmd *cobra.Command, cs *credentials.Store, uri url.URL) error {
	cmd.Printf("Discarding credentials for %s\n", uri.String())
	cs.Delete(uri)
	return cs.Flush()
}

func parseRemoteAndRefspec(cmd *cobra.Command, c *conf.Config, args []string) (string, *conf.Remote, []*conf.Refspec, error) {
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

type Ref struct {
	Src   string
	Dst   string
	Force bool
}

func identifyRefsToFetch(client *apiclient.Client, specs []*conf.Refspec) (refs []*Ref, dstRefs, maybeSaveTags map[string][]byte, advertised [][]byte, err error) {
	m, err := client.GetRefs()
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
				refs = append(refs, &Ref{
					r, dst, spec.Force,
				})
				covered = true
			}
		}
		if !covered && strings.HasPrefix(r, "tags/") {
			maybeSaveTags[r] = sum
		}
	}
	return
}

func trimRefPrefix(r string) string {
	if strings.Contains(r, "/") && !strings.HasPrefix(r, "refs/") && !strings.HasPrefix(r, "heads/") &&
		!strings.HasPrefix(r, "tags/") && !strings.HasPrefix(r, "remotes/") {
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
	cmd *cobra.Command, u *conf.User, db objects.Store, rs ref.Store, remoteURL string,
	fetchedCommits [][]byte, refs []*Ref, dstRefs, maybeSaveTags map[string][]byte, force bool,
) ([]*Ref, error) {
	someFailed := false
	// if a remote tag point to an existing object then save that tag
	cm := bytesSliceToMap(fetchedCommits)
	for r, sum := range maybeSaveTags {
		if _, ok := cm[string(sum)]; ok || objects.CommitExist(db, sum) {
			_, err := ref.GetRef(rs, r)
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
		oldSum, _ := ref.GetRef(rs, r.Dst)
		sum := dstRefs[r.Dst]
		if bytes.Equal(oldSum, sum) {
			continue
		}
		if !remoteDisplayed {
			cmd.Printf("From %s\n", remoteURL)
			remoteDisplayed = true
		}
		if oldSum != nil && strings.HasPrefix(r.Dst, "tags/") {
			if force || r.Force {
				err := ref.SaveRef(rs, r.Dst, sum, u.Name, u.Email, "fetch", "updating tag")
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
			if strings.HasPrefix(r.Src, "tags/") {
				msg = "storing tag"
				what = "[new tag]"
			} else if strings.HasPrefix(r.Src, "heads/") {
				msg = "storing head"
				what = "[new branch]"
			} else {
				msg = "storing ref"
				what = "[new ref]"
			}
			err := ref.SaveRef(rs, r.Dst, sum, u.Name, u.Email, "fetch", msg)
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
			err := ref.SaveRef(rs, r.Dst, sum, u.Name, u.Email, "fetch", "fast-forward")
			qr := quickref(oldSum, sum, true)
			if err != nil {
				displayRefUpdate(cmd, '!', qr, "unable to update local ref", r.Src, r.Dst)
				someFailed = true
			} else {
				displayRefUpdate(cmd, ' ', qr, "", r.Src, r.Dst)
				savedRefs = append(savedRefs, r)
			}
		} else if force || r.Force {
			err := ref.SaveRef(rs, r.Dst, sum, u.Name, u.Email, "fetch", "forced-update")
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

func fetchObjects(cmd *cobra.Command, db objects.Store, rs ref.Store, client *apiclient.Client, advertised [][]byte) (fetchedCommits [][]byte, err error) {
	ses, err := apiclient.NewUploadPackSession(db, rs, client, advertised, 0)
	if err != nil {
		if err.Error() == "nothing wanted" {
			err = nil
			return
		}
		return
	}
	return ses.Start()
}

func fetch(cmd *cobra.Command, db objects.Store, rs ref.Store, u *conf.User, remote, token string, cr *conf.Remote, specs []*conf.Refspec, force bool) error {
	client, err := apiclient.NewClient(cr.URL, apiclient.WithAuthorization(token))
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
