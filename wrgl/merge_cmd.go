// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"bytes"
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
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/conf"
	"github.com/wrgl/core/pkg/diff"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/merge"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
	"github.com/wrgl/core/pkg/slice"
	"github.com/wrgl/core/pkg/widgets"
	"github.com/wrgl/core/wrgl/utils"
)

func getTable(db objects.Store, comSum []byte) (sum []byte, tbl *objects.Table, err error) {
	com, err := objects.GetCommit(db, comSum)
	if err != nil {
		return
	}
	tbl, err = objects.GetTable(db, com.Table)
	if err != nil {
		return
	}
	return com.Table, tbl, nil
}

func mergeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "merge BRANCH COMMIT...",
		Short: "Merge two or more data histories together. If merge is successful then create a merge commit under BRANCH.",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := getRepoDir(cmd)
			c, err := utils.AggregateConfig(rd.FullPath)
			if err != nil {
				return err
			}
			ensureUserSet(cmd, c)
			defer setupDebug(cmd)()
			db, err := rd.OpenObjectsStore()
			if err != nil {
				return err
			}
			defer db.Close()
			rs := rd.OpenRefStore()
			noCommit, err := cmd.Flags().GetBool("no-commit")
			if err != nil {
				return err
			}
			noGUI, err := cmd.Flags().GetBool("no-gui")
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
			return runMerge(cmd, c, db, rs, args, noCommit, noGUI, commitCSV, numWorkers, message, pk)
		},
	}
	cmd.Flags().Bool("no-commit", false, "perform the merge but don't create a merge commit, instead output merge result to file MERGE_SUM1_SUM2_..._SUMn.csv")
	cmd.Flags().Bool("no-gui", false, "don't show mergetool, instead output conflicts (and resolved rows) to file CONFLICTS_SUM1_SUM2_..._SUMn.csv")
	cmd.Flags().String("commit-csv", "", "don't perform merge, just create a merge commit with the specified CSV file")
	cmd.Flags().StringP("message", "m", "", "merge commit message")
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "merge commit primary key. This is only used when --commit-csv is in use. If this isn't specified then primary key is the same as BRANCH HEAD's")
	cmd.Flags().IntP("num-workers", "n", runtime.GOMAXPROCS(0), "number of CPU threads to utilize (default to GOMAXPROCS)")
	return cmd
}

func runMerge(cmd *cobra.Command, c *conf.Config, db objects.Store, rs ref.Store, args []string, noCommit, noGUI bool, commitCSV string, numWorkers int, message string, pk []string) error {
	name, sum, _, err := ref.InterpretCommitName(db, rs, args[0], true)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(name, "refs/heads/") {
		return fmt.Errorf("%q is not a branch name", args[0])
	}
	commits := [][]byte{sum}
	commitNames := []string{trimRefPrefix(name)}
	for _, s := range args[1:] {
		name, sum, _, err := ref.InterpretCommitName(db, rs, s, true)
		if err != nil {
			return err
		}
		commits = append(commits, sum)
		commitNames = append(commitNames, trimRefPrefix(name))
	}
	baseCommit, err := ref.SeekCommonAncestor(db, commits...)
	if err != nil {
		return err
	}
	nonAncestralCommits := [][]byte{}
	for _, sum := range commits {
		if !bytes.Equal(sum, baseCommit) {
			nonAncestralCommits = append(nonAncestralCommits, sum)
		}
	}
	commits = nonAncestralCommits
	if len(commits) == 0 {
		cmd.Println("All commits are identical, nothing to merge")
		return nil
	} else if len(commits) == 1 {
		err = ref.SaveRef(rs, name[5:], commits[0], c.User.Name, c.User.Email, "merge", "fast-forward")
		if err != nil {
			return err
		}
		cmd.Printf("Fast forward to %s\n", hex.EncodeToString(commits[0])[:7])
		return nil
	}

	baseSum, baseT, err := getTable(db, baseCommit)
	if err != nil {
		return err
	}
	otherTs := make([]*objects.Table, len(commits))
	otherSums := make([][]byte, len(commits))
	for i, sum := range commits {
		otherSums[i], otherTs[i], err = getTable(db, sum)
		if err != nil {
			return err
		}
	}

	if len(pk) == 0 {
		pk = otherTs[0].PrimaryKey()
	}

	if commitCSV != "" {
		file, err := os.Open(commitCSV)
		if err != nil {
			return err
		}
		sum, err := ingest.IngestTable(db, file, file.Name(), pk, 0, numWorkers, cmd.OutOrStdout())
		if err != nil {
			return err
		}
		return createMergeCommit(cmd, db, rs, commitNames, sum, commits, message, c)
	}

	buf, err := diff.BlockBufferWithSingleStore(db, append([]*objects.Table{baseT}, otherTs...))
	if err != nil {
		return err
	}
	rowCollector, cleanup, err := merge.CreateRowCollector(db, baseT)
	if err != nil {
		return err
	}
	defer cleanup()
	merger, err := merge.NewMerger(db, rowCollector, buf, 65*time.Millisecond, baseT, otherTs, baseSum, otherSums)
	if err != nil {
		return err
	}
	defer merger.Close()

	if noGUI {
		return outputConflicts(cmd, db, buf, merger, commitNames, baseCommit, commits)
	} else {
		cd, merges, err := collectMergeConflicts(cmd, merger)
		if err != nil {
			return err
		}
		var removedCols map[int]struct{}
		if len(merges) == 0 {
			removedCols = map[int]struct{}{}
			for _, layer := range cd.Removed {
				for col := range layer {
					removedCols[int(col)] = struct{}{}
				}
			}
		} else {
			removedCols, err = displayMergeApp(cmd, buf, merger, commitNames, commits, baseCommit, cd, merges)
			if err != nil {
				return err
			}
		}
		if noCommit {
			return saveMergeResultToCSV(cmd, merger, removedCols, commits)
		} else {
			return commitMergeResult(cmd, db, rs, merger, removedCols, numWorkers, commitNames, commits, message, c)
		}
	}
}

func outputConflicts(cmd *cobra.Command, db objects.Store, buf *diff.BlockBuffer, merger *merge.Merger, commitNames []string, baseSum []byte, commits [][]byte) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	filename := mergeCSVName("CONFLICTS", commits)
	f, err := os.Create(path.Join(wd, filename))
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)

	baseName := fmt.Sprintf("BASE %s", hex.EncodeToString(baseSum)[:7])
	names := make([]string, len(commitNames))
	for i, name := range commitNames {
		names[i] = fmt.Sprintf("%s (%s)", name, hex.EncodeToString(commits[i])[:7])
	}

	mc, err := merger.Start()
	if err != nil {
		return err
	}
	cd := (<-mc).ColDiff
	columns := append([]string{""}, merger.Columns(nil)...)
	err = w.Write(columns)
	if err != nil {
		return err
	}
	for i, name := range names {
		row := make([]string, cd.Len()+1)
		row[0] = "COLUMNS: " + name
		for j := 1; j < len(row); j++ {
			if _, ok := cd.Added[i][uint32(j-1)]; ok {
				row[j] = "NEW"
			} else if _, ok := cd.Removed[i][uint32(j-1)]; ok {
				row[j] = "REMOVED"
			}
		}
		err = w.Write(row)
		if err != nil {
			return err
		}
	}
	for m := range mc {
		if m.Base != nil {
			blk, off := diff.RowToBlockAndOffset(m.BaseOffset)
			row, err := buf.GetRow(0, blk, off)
			if err != nil {
				return err
			}
			row = append([]string{baseName}, cd.RearrangeBaseRow(row)...)
			err = w.Write(row)
			if err != nil {
				return err
			}
		}
		for i, sum := range m.Others {
			if sum == nil && m.Base != nil {
				row := make([]string, cd.Len()+1)
				row[0] = names[i]
				txt := fmt.Sprintf("REMOVED IN %s", hex.EncodeToString(commits[i])[:7])
				for j := 1; j < len(row); j++ {
					row[j] = txt
				}
				err = w.Write(row)
				if err != nil {
					return err
				}
			} else if sum != nil {
				blk, off := diff.RowToBlockAndOffset(m.OtherOffsets[i])
				row, err := buf.GetRow(byte(i+1), blk, off)
				row = cd.RearrangeBaseRow(row)
				row = append([]string{names[i]}, row...)
				err = w.Write(row)
				if err != nil {
					return err
				}
			}
		}
		if len(m.ResolvedRow) > 0 {
			row := append([]string{"RESOLUTION"}, m.ResolvedRow...)
			err = w.Write(row)
			if err != nil {
				return err
			}
		}
		err = merger.SaveResolvedRow(m.PK, nil)
		if err != nil {
			return err
		}
	}
	if err = merger.Error(); err != nil {
		return err
	}
	rc, _, err := merger.SortedBlocks(nil)
	if err != nil {
		return err
	}
	for blk := range rc {
		for _, row := range blk.Block {
			row = append([]string{""}, row...)
			err = w.Write(row)
			if err != nil {
				return err
			}
		}
	}
	err = merger.Error()
	if err != nil {
		return err
	}

	w.Flush()
	err = f.Close()
	if err != nil {
		return err
	}
	cmd.Printf("saved conflicts to file %s\n", filename)
	return nil
}

func mergeCSVName(prefix string, commits [][]byte) string {
	sums := make([]string, len(commits))
	for i, b := range commits {
		sums[i] = hex.EncodeToString(b)[:7]
	}
	return fmt.Sprintf("%s_%s.csv", prefix, strings.Join(sums, "_"))
}

func saveMergeResultToCSV(cmd *cobra.Command, merger *merge.Merger, removedCols map[int]struct{}, commits [][]byte) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	name := path.Join(wd, mergeCSVName("MERGE", commits))
	f, err := os.Create(name)
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
	blocks, _, err := merger.SortedBlocks(removedCols)
	if err != nil {
		return err
	}
	bar := pbar(-1, fmt.Sprintf("saving merge result to %s", name), cmd.OutOrStdout(), cmd.ErrOrStderr())
	for blk := range blocks {
		for _, row := range blk.Block {
			err = w.Write(row)
			if err != nil {
				return err
			}
			err = bar.Add(1)
			if err != nil {
				return err
			}
		}
	}
	return bar.Finish()
}

func commitMergeResult(
	cmd *cobra.Command,
	db objects.Store,
	rs ref.Store,
	merger *merge.Merger,
	removedCols map[int]struct{},
	numWorkers int,
	commitNames []string,
	commits [][]byte,
	message string,
	c *conf.Config,
) error {
	columns := merger.Columns(removedCols)
	pk, err := slice.KeyIndices(columns, merger.PK())
	if err != nil {
		return err
	}
	blocks, rowsCount, err := merger.SortedBlocks(removedCols)
	if err != nil {
		return err
	}
	sum, err := ingest.IngestTableFromBlocks(db, columns, pk, rowsCount, blocks, numWorkers, cmd.OutOrStdout())
	if err != nil {
		return err
	}
	return createMergeCommit(cmd, db, rs, commitNames, sum, commits, message, c)
}

func createMergeCommit(cmd *cobra.Command, db objects.Store, rs ref.Store, commitNames []string, sum []byte, parents [][]byte, message string, c *conf.Config) error {
	if message == "" {
		quotedNames := []string{}
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
	buf := bytes.NewBuffer(nil)
	_, err := commit.WriteTo(buf)
	if err != nil {
		return err
	}
	commitSum, err := objects.SaveCommit(db, buf.Bytes())
	if err != nil {
		return err
	}
	err = ref.CommitMerge(rs, commitNames[0], commitSum, commit)
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

func collectMergeConflicts(cmd *cobra.Command, merger *merge.Merger) (*objects.ColDiff, []*merge.Merge, error) {
	var bar *progressbar.ProgressBar
	mch, err := merger.Start()
	if err != nil {
		return nil, nil, err
	}
	pch := merger.Progress.Chan()
	go merger.Progress.Run()
	merges := []*merge.Merge{}
mainLoop:
	for {
		select {
		case p := <-pch:
			if bar == nil {
				bar = pbar(p.Total, "collecting merge conflicts", cmd.OutOrStdout(), cmd.OutOrStderr())
			}
			bar.Set64(p.Progress)
		case m, ok := <-mch:
			if !ok {
				break mainLoop
			}
			merges = append(merges, m)
		}
	}
	merger.Progress.Stop()
	if bar != nil {
		if err = bar.Finish(); err != nil {
			return nil, nil, err
		}
	}
	if err = merger.Error(); err != nil {
		return nil, nil, err
	}
	return merges[0].ColDiff, merges[1:], nil
}

func displayMergeApp(cmd *cobra.Command, buf *diff.BlockBuffer, merger *merge.Merger, commitNames []string, commitSums [][]byte, baseSum []byte, cd *objects.ColDiff, merges []*merge.Merge) (map[int]struct{}, error) {
	app := tview.NewApplication()
	mergeApp := widgets.NewMergeApp(buf, merger, app, commitNames, commitSums, baseSum)
	mergeApp.InitializeTable(cd, merges)
	app.SetRoot(mergeApp.Flex, true).
		SetFocus(mergeApp.Table).
		SetBeforeDrawFunc(func(screen tcell.Screen) bool {
			mergeApp.BeforeDraw(screen)
			return false
		}).
		EnableMouse(true)

	cancel := redrawEvery(app, 65*time.Millisecond)
	defer cancel()

	err := app.Run()
	if err != nil {
		return nil, err
	}
	if !mergeApp.Finished {
		cmd.Println("merge aborted")
		os.Exit(0)
	}
	return mergeApp.RemovedCols, nil
}
