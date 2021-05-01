package main

import (
	"encoding/hex"
	"fmt"

	"github.com/rivo/tview"
	"github.com/spf13/cobra"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
	"github.com/wrgl/core/pkg/widgets"
)

func newCatFileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cat-file OBJECT_HASH",
		Short: "Provide content information for repository object",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			hash, err := hex.DecodeString(args[0])
			if err != nil {
				return err
			}
			rd := getRepoDir(cmd)
			quitIfRepoDirNotExist(cmd, rd)
			kvStore, err := rd.OpenKVStore()
			if err != nil {
				return err
			}
			defer kvStore.Close()
			commit, err := versioning.GetCommit(kvStore, hash)
			if err == nil {
				return catCommit(cmd, commit)
			}
			ts, err := table.ReadSmallStore(kvStore, seed, hash)
			if err == nil {
				return catTable(cmd, ts)
			}
			row, err := table.GetRow(kvStore, hash)
			if err == nil {
				return catRow(cmd, row)
			}
			return fmt.Errorf("unrecognized hash")
		},
	}
	return cmd
}

func catCommit(cmd *cobra.Command, commit *objects.Commit) error {
	app := tview.NewApplication()
	textView := tview.NewTextView().
		SetDynamicColors(true)
	fmt.Fprintf(textView, "[yellow]table[white]  %s\n", hex.EncodeToString(commit.Table))
	fmt.Fprintf(textView, "[yellow]author[white] %s <%s>\n", commit.AuthorName, commit.AuthorEmail)
	fmt.Fprintf(textView, "[yellow]time[white]   %d %s\n\n", commit.Time.Unix(), commit.Time.Format("-0700"))
	fmt.Fprintln(textView, commit.Message)
	return app.SetRoot(textView, true).SetFocus(textView).Run()
}

func catTable(cmd *cobra.Command, ts table.Store) error {
	cols := ts.Columns()
	pk := ts.PrimaryKey()
	reader, err := ts.NewRowHashReader(0, 0)
	if err != nil {
		return err
	}
	n, err := ts.NumRows()
	if err != nil {
		return err
	}
	app := tview.NewApplication()
	textView := widgets.NewPaginatedTextView().
		SetPullText(func() ([]byte, error) {
			pkh, rh, err := reader.Read()
			if err != nil {
				return nil, err
			}
			return []byte(fmt.Sprintf("[aquaMarine]%s[white] : %s\n", hex.EncodeToString(pkh), hex.EncodeToString(rh))), nil
		}).
		SetDynamicColors(true).
		SetChangedFunc(func() {
			app.Draw()
		})
	fmt.Fprintf(textView, "[yellow]columns[white] ([cyan]%d[white])\n\n", len(cols))
	for _, col := range cols {
		fmt.Fprintf(textView, "%s\n", col)
	}
	if len(pk) > 0 {
		fmt.Fprintf(textView, "\n[yellow]primary key[white] ([cyan]%d[white])\n\n", len(pk))
		for _, col := range pk {
			fmt.Fprintf(textView, "%s\n", col)
		}
	}
	fmt.Fprintf(textView, "\n[yellow]rows[white] ([cyan]%d[white])\n\n", n)
	err = textView.PullText()
	if err != nil {
		return err
	}
	return app.SetRoot(textView, true).SetFocus(textView).Run()
}

func catRow(cmd *cobra.Command, row []byte) error {
	dec := objects.NewStrListDecoder(false)
	cells := dec.Decode(row)
	app := tview.NewApplication()
	textView := tview.NewTextView().
		SetDynamicColors(true)
	fmt.Fprintf(textView, "[yellow]cells[white] ([cyan]%d[white])\n\n", len(cells))
	for _, cell := range cells {
		fmt.Fprintf(textView, "%s\n", cell)
	}
	return app.SetRoot(textView, true).SetFocus(textView).Run()
}
