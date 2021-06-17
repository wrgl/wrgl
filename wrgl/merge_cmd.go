// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"context"
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

func displayMergeApp(cmd *cobra.Command, db kv.DB, fs kv.FileStore, merger *merge.Merger, commitNames []string, commitSums [][]byte, baseSum []byte) error {
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

	return app.Run()
}
