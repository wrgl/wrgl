package main

import (
	"bytes"
	"os"
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
			c, err := versioning.OpenConfig(false, wrglDir)
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
			if all {
				for k, v := range c.Remote {
					err := fetch(cmd, db, fs, k, v, v.Fetch, dryRun)
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
			return fetch(cmd, db, fs, remote, rem, specs, dryRun)
		},
	}
	cmd.Flags().Bool("all", false, "Fetch all remotes.")
	cmd.Flags().Bool("dry-run", false, "Show what would be done, without making any changes.")
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

func identifyRefsToFetch(client *packclient.Client, specs []*versioning.Refspec) (refs map[string][]byte, advertised [][]byte, err error) {
	m, err := client.GetRefsInfo()
	if err != nil {
		return
	}
	refs = map[string][]byte{}
	for ref, sum := range m {
		for _, spec := range specs {
			dst := spec.DstForRef(ref)
			if dst != "" {
				refs[dst] = sum
			}
		}
	}
	for _, sum := range refs {
		advertised = append(advertised, sum)
	}
	return
}

func saveFetchedRefs(db kv.Store, refs map[string][]byte) error {
	for ref, sum := range refs {
		err := versioning.SaveRef(db, ref[4:], sum)
		if err != nil {
			return err
		}
	}
	return nil
}

func fetch(cmd *cobra.Command, db kv.Store, fs kv.FileStore, remote string, cr *versioning.ConfigRemote, specs []*versioning.Refspec, dryRun bool) error {
	client, err := packclient.NewClient(cr.URL)
	if err != nil {
		return err
	}
	refs, advertised, err := identifyRefsToFetch(client, specs)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	oc := make(chan *packclient.Object, 100)
	neg, err := packclient.NewNegotiator(db, fs, &wg, client, advertised, oc)
	if err != nil {
		if err.Error() == "nothing wanted" {
			return nil
		}
		return err
	}
	done := make(chan bool)
	ec := make(chan error)
	go exitOnError(cmd, done, ec)
	go saveObjects(db, fs, &wg, 0, oc, ec)
	err = neg.Start()
	if err != nil {
		return err
	}
	wg.Wait()
	done <- true
	return saveFetchedRefs(db, refs)
}
