// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/diff"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/merge"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/pbar"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/slice"
	"github.com/wrgl/wrgl/pkg/sorter"
	"github.com/wrgl/wrgl/pkg/widgets"
)

func getTable(db objects.Store, rs ref.Store, comSum []byte) (sum []byte, tbl *objects.Table, err error) {
	com, err := objects.GetCommit(db, comSum)
	if err != nil {
		return
	}
	tbl, err = utils.GetTable(db, rs, com)
	if err != nil {
		return
	}
	return com.Table, tbl, nil
}

func mergeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "merge BRANCH COMMIT",
		Short: "Merge two commits together.",
		Long:  "Merge two commits together using merge UI. If merge is successful then create a merge commit under BRANCH.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "merge two branches",
				Line:    "wrgl merge branch-1 branch-2",
			},
			{
				Comment: "merge a commit into a branch",
				Line:    "wrgl merge my-branch 43a5f3447e82b53a2574ef5af470df96",
			},
			{
				Comment: "perform merge but don't create a merge commit, output result to file MERGE_SUM1_SUM2.csv instead",
				Line:    "wrgl merge branch-1 branch-2 --no-commit",
			},
			{
				Comment: "don't show merge UI, output conflicts and resolved rows to CONFLICTS_SUM1_SUM2.csv instead",
				Line:    "wrgl merge branch-1 branch-2 --no-gui",
			},
			{
				Comment: "create a merge commit from an already resolved CSV file",
				Line:    "wrgl merge branch-1 branch-2 --commit-csv resolved.csv",
			},
		}),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			s := conffs.NewStore(rd.FullPath, conffs.AggregateSource, "")
			c, err := s.Open()
			if err != nil {
				return err
			}
			if err := utils.EnsureUserSet(cmd, c); err != nil {
				return err
			}
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
			ff, err := getFastForward(cmd, c)
			if err != nil {
				return err
			}
			return runMerge(cmd, c, db, rs, args, noCommit, noGUI, ff, commitCSV, numWorkers, message, pk)
		},
	}
	cmd.Flags().Bool("no-commit", false, "perform the merge but don't create a merge commit, instead output merge result to file MERGE_SUM1_SUM2_..._SUMn.csv")
	cmd.Flags().Bool("no-gui", false, "don't show mergetool, instead output conflicts (and resolved rows) to file CONFLICTS_SUM1_SUM2_..._SUMn.csv")
	cmd.Flags().String("commit-csv", "", "don't perform merge, just create a merge commit with the specified CSV file")
	cmd.Flags().StringP("message", "m", "", "merge commit message")
	cmd.Flags().StringSliceP("primary-key", "p", []string{}, "merge commit primary key. This is only used when --commit-csv is in use. If this isn't specified then primary key is the same as BRANCH HEAD's")
	cmd.Flags().IntP("num-workers", "n", runtime.GOMAXPROCS(0), "number of CPU threads to utilize (default to GOMAXPROCS)")
	cmd.Flags().Bool("ff", false, "when merging a descendant commit into a branch, don't create a merge commit but simply fast-forward branch to the descendant commit. Create an extra merge commit otherwise. This is the default behavior unless merge.fastForward is configured.")
	cmd.Flags().Bool("no-ff", false, "always create a merge commit, even when a simple fast-forward is possible. This is the default when merge.fastFoward is set to \"never\".")
	cmd.Flags().Bool("ff-only", false, "only allow fast-forward merges. This is the default when merge.fastForward is set to \"only\".")
	cmd.Flags().String("delimiter", "", "CSV delimiter during commit with --commit-csv, defaults to comma")
	return cmd
}

func getFastForward(cmd *cobra.Command, c *conf.Config) (conf.FastForward, error) {
	defFF, err := cmd.Flags().GetBool("ff")
	if err != nil {
		return "", err
	}
	noFF, err := cmd.Flags().GetBool("no-ff")
	if err != nil {
		return "", err
	}
	ffOnly, err := cmd.Flags().GetBool("ff-only")
	if err != nil {
		return "", err
	}
	ff := c.MergeFastForward()
	if defFF {
		ff = conf.FF_Default
	} else if noFF {
		ff = conf.FF_Never
	} else if ffOnly {
		ff = conf.FF_Only
	}
	return ff, nil
}

func runMerge(
	cmd *cobra.Command, c *conf.Config, db objects.Store, rs ref.Store, args []string, noCommit, noGUI bool,
	ff conf.FastForward, commitCSV string, numWorkers int, message string, pk []string,
) error {
	name, sum, _, err := ref.InterpretCommitName(db, rs, args[0], true)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(name, "heads/") {
		return fmt.Errorf("%q is not a branch name", args[0])
	}
	commits := [][]byte{sum}
	commitNames := []string{displayableCommitName(args[0], sum)}
	for _, s := range args[1:] {
		_, sum, com, err := ref.InterpretCommitName(db, rs, s, true)
		if err != nil {
			return err
		}
		if !objects.TableExist(db, com.Table) {
			return utils.ErrTableNotFound(db, rs, com)
		}
		commits = append(commits, sum)
		commitNames = append(commitNames, displayableCommitName(s, sum))
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
	if len(nonAncestralCommits) == 0 {
		cmd.Println("All commits are identical, nothing to merge")
		return nil
	} else if len(nonAncestralCommits) == 1 {
		if ff == conf.FF_Never {
			com, err := objects.GetCommit(db, nonAncestralCommits[0])
			if err != nil {
				return err
			}
			return createMergeCommit(cmd, db, rs, commitNames, com.Table, commits, message, c)
		}
		err = ref.SaveRef(rs, name, nonAncestralCommits[0], c.User.Name, c.User.Email, "merge", "fast-forward", nil)
		if err != nil {
			return err
		}
		cmd.Printf("Fast forward to %s\n", hex.EncodeToString(nonAncestralCommits[0])[:7])
		return nil
	} else if ff == conf.FF_Only {
		return fmt.Errorf("merge rejected (non-fast-forward)")
	}
	commits = nonAncestralCommits

	baseSum, baseT, err := getTable(db, rs, baseCommit)
	if err != nil {
		return err
	}
	otherTs := make([]*objects.Table, len(commits))
	otherSums := make([][]byte, len(commits))
	for i, sum := range commits {
		otherSums[i], otherTs[i], err = getTable(db, rs, sum)
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
		delim, err := utils.GetRuneFromFlag(cmd, "delimiter")
		if err != nil {
			return err
		}
		sum, err := ingestTable(
			cmd, db, file, pk, false,
			[]sorter.SorterOption{sorter.WithDelimiter(delim)},
			[]ingest.InserterOption{ingest.WithNumWorkers(numWorkers)},
		)
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
		row[0] = "COLUMNS IN " + name
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
	merges := []*merge.Merge{}
	for m := range mc {
		merges = append(merges, m)
	}
	// sort to make test stable
	sort.SliceStable(merges, func(i, j int) bool {
		if merges[i].Base == nil && merges[j].Base != nil {
			return true
		}
		if merges[j].Base == nil && merges[i].Base != nil {
			return false
		}
		return string(merges[i].Base) < string(merges[j].Base)
	})
	for _, m := range merges {
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
				if err != nil {
					return err
				}
				row = cd.RearrangeRow(i, row)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rc, err := merger.SortedRows(ctx, nil)
	if err != nil {
		return err
	}
	for blk := range rc {
		for _, row := range blk.Rows {
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocks, err := merger.SortedRows(ctx, removedCols)
	if err != nil {
		return err
	}
	return utils.WithProgressBar(cmd, false, func(cmd *cobra.Command, barContainer pbar.Container) error {
		bar := barContainer.NewBar(-1, fmt.Sprintf("saving merge result to %s", name), 0)
		defer bar.Done()
		for blk := range blocks {
			for _, row := range blk.Rows {
				err = w.Write(row)
				if err != nil {
					return err
				}
				bar.Incr()
			}
		}
		return nil
	})
}

func ingestTable(
	cmd *cobra.Command,
	db objects.Store,
	file io.ReadCloser,
	pk []string,
	quiet bool,
	sorterOpts []sorter.SorterOption,
	inserterOpts []ingest.InserterOption,
) (tableSum []byte, err error) {
	err = utils.WithProgressBar(cmd, quiet, func(cmd *cobra.Command, barContainer pbar.Container) error {
		sortPT := barContainer.NewBar(-1, "sorting", 0)
		blkPT := barContainer.NewBar(-1, "saving blocks", 0)
		defer sortPT.Done()
		defer blkPT.Done()
		s, err := sorter.NewSorter(
			append(sorterOpts, sorter.WithProgressBar(sortPT))...,
		)
		if err != nil {
			return err
		}
		tableSum, err = ingest.IngestTable(db, s, file, pk,
			append(inserterOpts, ingest.WithProgressBar(blkPT))...,
		)
		return err
	})
	return
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocks, err := merger.SortedBlocks(ctx, removedCols)
	if err != nil {
		return err
	}
	var sum []byte
	if err := utils.WithProgressBar(cmd, false, func(cmd *cobra.Command, barContainer pbar.Container) error {
		blkPT := barContainer.NewBar(-1, "saving blocks", 0)
		defer blkPT.Done()
		s, err := sorter.NewSorter()
		if err != nil {
			return err
		}
		sum, err = ingest.IngestTableFromBlocks(db, s, columns, pk, blocks,
			ingest.WithNumWorkers(numWorkers),
			ingest.WithProgressBar(blkPT),
		)
		return err
	}); err != nil {
		return err
	}
	tbl, err := objects.GetTable(db, sum)
	if err != nil {
		return err
	}
	if err = ingest.ProfileTable(db, sum, tbl); err != nil {
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

func collectMergeConflicts(cmd *cobra.Command, merger *merge.Merger) (*diff.ColDiff, []*merge.Merge, error) {
	mch, err := merger.Start()
	if err != nil {
		return nil, nil, err
	}
	pch := merger.Progress.Start()
	merges := []*merge.Merge{}
	if err := utils.WithProgressBar(cmd, false, func(cmd *cobra.Command, barContainer pbar.Container) error {
		var bar pbar.Bar
	mainLoop:
		for {
			select {
			case p := <-pch:
				if bar == nil {
					bar = barContainer.NewBar(p.Total, "collecting merge conflicts", 0)
					defer bar.Done()
				}
				bar.SetCurrent(p.Progress)
			case m, ok := <-mch:
				if !ok {
					break mainLoop
				}
				merges = append(merges, m)
			}
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	merger.Progress.Stop()
	if err = merger.Error(); err != nil {
		return nil, nil, err
	}
	return merges[0].ColDiff, merges[1:], nil
}

func displayMergeApp(cmd *cobra.Command, buf *diff.BlockBuffer, merger *merge.Merger, commitNames []string, commitSums [][]byte, baseSum []byte, cd *diff.ColDiff, merges []*merge.Merge) (map[int]struct{}, error) {
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
