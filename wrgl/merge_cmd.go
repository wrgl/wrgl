// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/merge"
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
		Short: "Join two or more data histories together",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			rd := getRepoDir(cmd)
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
			return displayMergeApp(cmd, kvStore, fs, merger, commitNames, commits, baseCommit)
		},
	}
	return cmd
}

func createMergeTitleBar(commitNames []string, baseSum []byte) *tview.TextView {
	titleBar := tview.NewTextView().SetDynamicColors(true)
	sl := make([]string, len(commitNames))
	for i, s := range commitNames {
		sl[i] = fmt.Sprintf("[yellow]%s[white]", s)
	}
	fmt.Fprintf(
		titleBar, "Merging %s (base [yellow]%s[white])", strings.Join(sl, ", "), hex.EncodeToString(baseSum)[:7],
	)
	return titleBar
}

func collectMergeConflicts(flex *tview.Flex, merger *merge.Merger) ([]*merge.Merge, error) {
	pBar := widgets.NewProgressBar("Counting merge conflicts...")
	flex.AddItem(pBar, 1, 1, false)
	mch, err := merger.Start()
	if err != nil {
		return nil, err
	}
	pch := merger.Progress.Chan()
	go merger.Progress.Run()
	merges := []*merge.Merge{}
mainLoop:
	for {
		select {
		case p := <-pch:
			pBar.SetTotal(p.Total)
			pBar.SetCurrent(p.Progress)
		case m, ok := <-mch:
			if !ok {
				break mainLoop
			}
			merges = append(merges, &m)
		}
	}
	merger.Progress.Stop()
	flex.RemoveItem(pBar)
	if err = merger.Error(); err != nil {
		return nil, err
	}
	return merges, nil
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

func displayMergeTable(app *tview.Application, flex *tview.Flex, mt *widgets.MergeTable, merger *merge.Merger, merges []*merge.Merge) {
	countBar := tview.NewTextView().SetDynamicColors(true)
	n := len(merges) - 1
	resolved := 0
	updateCountBar := func() {
		pct := float32(resolved) / float32(n) * 100
		fmt.Fprintf(countBar, "Resolve %d / %d (%.1f%%) conflicts", resolved, n, pct)
	}
	updateCountBar()
	usageBar := widgets.MergeTableUsageBar()
	flex.AddItem(countBar, 1, 1, false).
		AddItem(mt, 0, 1, true).
		AddItem(usageBar, 1, 1, false)
	mt.ShowMerge(merges[resolved])
	app.SetBeforeDrawFunc(func(screen tcell.Screen) bool {
		usageBar.BeforeDraw(screen, flex)
		return false
	}).SetFocus(mt)
	app.Draw()
}

func displayMergeApp(cmd *cobra.Command, db kv.DB, fs kv.FileStore, merger *merge.Merger, commitNames []string, commitSums [][]byte, baseSum []byte) error {
	app := tview.NewApplication()
	titleBar := createMergeTitleBar(commitNames, baseSum)
	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(titleBar, 1, 1, false)
	app.SetRoot(flex, true).
		SetFocus(flex).
		EnableMouse(true)

	cancel := redrawEvery(app, 65*time.Millisecond)
	defer cancel()

	go func() {
		merges, err := collectMergeConflicts(flex, merger)
		if err != nil {
			panic(err)
		}
		mt, err := widgets.NewMergeTable(db, fs, commitNames, commitSums, baseSum, merges[0].ColDiff, func(resolvedRow []string) {

		})
		if err != nil {
			panic(err)
		}
		displayMergeTable(app, flex, mt, merger, merges[1:])
	}()

	return app.Run()
}
