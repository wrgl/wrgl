// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"context"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/merge"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/slice"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/pkg/widgets"
)

func getTable(db kv.DB, fs kv.FileStore, sum []byte) (table.Store, error) {
	com, err := versioning.GetCommit(db, sum)
	if err != nil {
		return nil, err
	}
	return table.ReadTable(db, fs, com.Table)
}

func mergeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "merge BRANCH COMMIT...",
		Short: "Merge two or more data histories together. If merge is successful then create a merge commit under BRANCH.",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := getRepoDir(cmd)
			c, err := versioning.AggregateConfig(rd.FullPath)
			if err != nil {
				return err
			}
			ensureUserSet(cmd, c)
			kvStore, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			defer kvStore.Close()
			fs := rd.OpenFileStore()
			name, sum, _, err := versioning.InterpretCommitName(kvStore, args[0], true)
			if err != nil {
				return err
			}
			if !strings.HasPrefix(name, "refs/heads/") {
				return fmt.Errorf("%q is not a branch name", args[0])
			}
			commits := [][]byte{sum}
			commitNames := []string{trimRefPrefix(name)}
			for _, s := range args[1:] {
				name, sum, _, err := versioning.InterpretCommitName(kvStore, s, true)
				if err != nil {
					return err
				}
				commits = append(commits, sum)
				commitNames = append(commitNames, trimRefPrefix(name))
			}
			baseCommit, err := versioning.SeekCommonAncestor(kvStore, commits...)
			if err != nil {
				return err
			}

			baseT, err := getTable(kvStore, fs, baseCommit)
			if err != nil {
				return err
			}
			otherTs := make([]table.Store, len(commits))
			for i, sum := range commits {
				otherTs[i], err = getTable(kvStore, fs, sum)
				if err != nil {
					return err
				}
			}

			noCommit, err := cmd.Flags().GetBool("no-commit")
			if err != nil {
				return err
			}
			noMergetool, err := cmd.Flags().GetBool("no-mergetool")
			if err != nil {
				return err
			}
			commitCSV, err := cmd.Flags().GetString("commit-csv")
			if err != nil {
				return err
			}
			numWorkers, err := cmd.Flags().GetInt("num-workers")
			if err != nil {
				return err
			}
			message, err := cmd.Flags().GetString("message")
			if err != nil {
				return err
			}
			pk, err := cmd.Flags().GetStringSlice("primary-key")
			if err != nil {
				return err
			}
			if len(pk) == 0 {
				otherTs[0].PrimaryKey()
			}

			if commitCSV != "" {
				file, err := os.Open(commitCSV)
				if err != nil {
					return err
				}
				defer file.Close()
				sum, err := ingestTable(cmd, kvStore, fs, numWorkers, file, pk)
				if err != nil {
					return err
				}
				return createMergeCommit(cmd, kvStore, fs, commitNames, sum, commits, message, c)
			}

			rowCollector, cleanup, err := merge.CreateRowCollector(kvStore, baseT)
			if err != nil {
				return err
			}
			defer cleanup()
			merger, err := merge.NewMerger(kvStore, fs, rowCollector, 65*time.Millisecond, baseT, otherTs...)
			if err != nil {
				return err
			}
			defer merger.Close()

			if noMergetool {

			} else {
				removedCols, err := displayMergeApp(cmd, kvStore, fs, merger, commitNames, commits, baseCommit)
				if err != nil {
					return err
				}
				if noCommit {
					return saveMergeResultToCSV(merger, removedCols, commits)
				} else {
					return commitMergeResult(cmd, kvStore, fs, merger, removedCols, numWorkers, commitNames, commits, message, c)
				}
			}
			return nil
		},
	}
	cmd.Flags().Bool("no-commit", false, "perform the merge but don't create a merge commit, instead output merge result to file MERGE_SUM1_SUM2_..._SUMn.csv")
	cmd.Flags().Bool("no-mergetool", false, "don't show mergetool, instead output conflicts (and resolved rows) to file CONFLICTS_SUM1_SUM2_..._SUMn.csv")
	cmd.Flags().String("commit-csv", "", "don't perform merge, just create a merge commit with the specified CSV file")
	cmd.Flags().StringP("message", "m", "", "merge commit message")
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "merge commit primary key. This is only used when --commit-csv is in use. If this isn't specified then primary key is the same as BRANCH HEAD's")
	cmd.Flags().IntP("num-workers", "n", runtime.GOMAXPROCS(0), "number of CPU threads to utilize (default to GOMAXPROCS)")
	return cmd
}

func mergeCSVName(commits [][]byte) string {
	sums := make([]string, len(commits))
	for i, b := range commits {
		sums[i] = hex.EncodeToString(b)[:7]
	}
	return fmt.Sprintf("MERGER_%s.csv", strings.Join(sums, "_"))
}

func saveMergeResultToCSV(merger *merge.Merger, removedCols map[int]struct{}, commits [][]byte) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	f, err := os.Create(path.Join(wd, mergeCSVName(commits)))
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	err = w.Write(merger.Columns(removedCols))
	if err != nil {
		return err
	}
	rows, err := merger.SortedRows(removedCols)
	if err != nil {
		return err
	}
	for row := range rows {
		err = w.Write(row)
		if err != nil {
			return err
		}
	}
	return nil
}

func commitMergeResult(
	cmd *cobra.Command,
	db kv.DB,
	fs kv.FileStore,
	merger *merge.Merger,
	removedCols map[int]struct{},
	numWorkers int,
	commitNames []string,
	commits [][]byte,
	message string,
	c *versioning.Config,
) error {
	columns := merger.Columns(removedCols)
	pk, err := slice.KeyIndices(columns, merger.PK())
	if err != nil {
		return err
	}
	rows, err := getRowsFromMerger(merger, removedCols)
	if err != nil {
		return err
	}
	tb := table.NewBuilder(db, fs, columns, pk, seed, 0)
	sum, err := ingest.NewIngestor(tb, seed, pk, numWorkers, cmd.OutOrStdout()).
		SetRowsChan(rows).
		Ingest()
	if err != nil {
		return err
	}
	return createMergeCommit(cmd, db, fs, commitNames, sum, commits, message, c)
}

func getRowsFromMerger(m *merge.Merger, removedCols map[int]struct{}) (chan ingest.Row, error) {
	rows := make(chan ingest.Row, 1000)
	ch, err := m.SortedRows(removedCols)
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(rows)
		n := 0
		for sl := range ch {
			rows <- ingest.Row{Record: sl, Index: n}
			n++
		}
	}()
	return rows, nil
}

func createMergeCommit(cmd *cobra.Command, db kv.DB, fs kv.FileStore, commitNames []string, sum []byte, parents [][]byte, message string, c *versioning.Config) error {
	if message == "" {
		quotedNames := make([]string, len(commitNames)-1)
		for _, name := range commitNames[1:] {
			quotedNames = append(quotedNames, fmt.Sprintf("%q", name))
		}
		message = fmt.Sprintf("Merge %s into %q", strings.Join(quotedNames, ", "), commitNames[0])
	}
	commit := &objects.Commit{
		Table:       sum,
		Message:     message,
		Time:        time.Now(),
		AuthorEmail: c.User.Email,
		AuthorName:  c.User.Name,
		Parents:     parents,
	}
	commitSum, err := versioning.SaveCommit(db, seed, commit)
	if err != nil {
		return err
	}
	err = versioning.CommitMerge(db, fs, commitNames[0], commitSum, commit)
	if err != nil {
		return err
	}
	cmd.Printf("[%s %s] %s\n", commitNames[0], hex.EncodeToString(commitSum)[:7], message)
	return nil
}

func redrawEvery(app *tview.Application, d time.Duration) (cancel func()) {
	drawCtx, cancel := context.WithCancel(context.Background())
	ticker := time.NewTicker(d)
	go func() {
		for {
			select {
			case <-ticker.C:
				app.Draw()
			case <-drawCtx.Done():
				return
			}
		}
	}()
	return cancel
}

func displayMergeApp(cmd *cobra.Command, db kv.DB, fs kv.FileStore, merger *merge.Merger, commitNames []string, commitSums [][]byte, baseSum []byte) (map[int]struct{}, error) {
	app := tview.NewApplication()
	mergeApp := widgets.NewMergeApp(db, fs, merger, app, commitNames, commitSums, baseSum)
	app.SetRoot(mergeApp.Flex, true).
		SetFocus(mergeApp.Flex).
		SetBeforeDrawFunc(func(screen tcell.Screen) bool {
			mergeApp.BeforeDraw(screen)
			return false
		}).
		EnableMouse(true)

	cancel := redrawEvery(app, 65*time.Millisecond)
	defer cancel()

	go func() {
		err := mergeApp.CollectMergeConflicts()
		if err != nil {
			panic(err)
		}
		mergeApp.InitializeTable()
		app.SetFocus(mergeApp.Table)
	}()

	err := app.Run()
	if err != nil {
		return nil, err
	}
	if mergeApp.Aborted {
		cmd.Println("merge aborted")
	}
	return mergeApp.RemovedCols, nil
}
