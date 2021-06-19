// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/objects"
)

func createCSVFile(t *testing.T, content []string) (filePath string) {
	t.Helper()
	file, err := ioutil.TempFile("", "test_commit_*.csv")
	require.NoError(t, err)
	defer file.Close()
	for _, line := range content {
		_, err := fmt.Fprintln(file, line)
		require.NoError(t, err)
	}
	return file.Name()
}

func commitFile(t *testing.T, branchName, filePath, primaryKey string, args ...string) {
	t.Helper()
	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetArgs(append([]string{"commit", branchName, filePath, "commit message", "--primary-key", primaryKey, "-n", "1"}, args...))
	require.NoError(t, cmd.Execute())
}

func assertDiffOutput(t *testing.T, cmd *cobra.Command, diffs []*objects.Diff) {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	reader := objects.NewDiffReader(bytes.NewReader(buf.Bytes()))
	objs := []*objects.Diff{}
	for {
		obj, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		objs = append(objs, obj)
	}
	assert.Equal(t, diffs, objs)
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestDiffCmd(t *testing.T) {
	_, cleanup := createRepoDir(t)
	defer cleanup()

	fp1 := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp1)
	commitFile(t, "my-branch", fp1, "a")

	fp2 := createCSVFile(t, []string{
		"a,b,c",
		"1,q,e",
		"2,a,s",
		"4,s,d",
	})
	defer os.Remove(fp2)
	commitFile(t, "my-branch", fp2, "a")

	cmd := newRootCmd()
	cmd.SetArgs([]string{"diff", "my-branch", "my-branch^", "--raw"})
	assertDiffOutput(t, cmd, []*objects.Diff{
		{
			Type:   objects.DTRow,
			PK:     mustDecodeHex(t, "fd1c9513cc47feaf59fa9b76008f2521"),
			Sum:    mustDecodeHex(t, "472dc02a63f3a555b9b39cf6c953a3ea"),
			OldSum: mustDecodeHex(t, "60f1c744d65482e468bfac458a7131fe"),
		},
		{
			Type: objects.DTRow,
			PK:   mustDecodeHex(t, "c5e86ba7d7653eec345ae9b6d77ab0cc"),
			Sum:  mustDecodeHex(t, "2e57aba9da65dfa5a185c4cb72ead76f"),
		},
		{
			Type:   objects.DTRow,
			PK:     mustDecodeHex(t, "e3c37d3bfd03aef8fac2794539e39160"),
			OldSum: mustDecodeHex(t, "1c51f6044190122c554cc6794585e654"),
		},
	})

	cmd = newRootCmd()
	cmd.SetArgs([]string{"diff", "my-branch", "--raw"})
	assertDiffOutput(t, cmd, []*objects.Diff{
		{
			Type:   objects.DTRow,
			PK:     mustDecodeHex(t, "fd1c9513cc47feaf59fa9b76008f2521"),
			Sum:    mustDecodeHex(t, "472dc02a63f3a555b9b39cf6c953a3ea"),
			OldSum: mustDecodeHex(t, "60f1c744d65482e468bfac458a7131fe"),
		},
		{
			Type: objects.DTRow,
			PK:   mustDecodeHex(t, "c5e86ba7d7653eec345ae9b6d77ab0cc"),
			Sum:  mustDecodeHex(t, "2e57aba9da65dfa5a185c4cb72ead76f"),
		},
		{
			Type:   objects.DTRow,
			PK:     mustDecodeHex(t, "e3c37d3bfd03aef8fac2794539e39160"),
			OldSum: mustDecodeHex(t, "1c51f6044190122c554cc6794585e654"),
		},
	})
}

func TestDiffCmdNoRepoDir(t *testing.T) {
	fp1 := createCSVFile(t, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	})
	defer os.Remove(fp1)

	fp2 := createCSVFile(t, []string{
		"a,c,b",
		"1,e,q",
		"2,s,a",
		"4,d,s",
	})
	defer os.Remove(fp2)

	cmd := newRootCmd()
	cmd.SetArgs([]string{"diff", fp2, fp1, "--raw", "--primary-key", "a"})
	assertDiffOutput(t, cmd, []*objects.Diff{
		{
			Type:   objects.DTRow,
			PK:     mustDecodeHex(t, "fd1c9513cc47feaf59fa9b76008f2521"),
			Sum:    mustDecodeHex(t, "de435956a13f72b660e50db3320a2ede"),
			OldSum: mustDecodeHex(t, "60f1c744d65482e468bfac458a7131fe"),
		},
		{
			Type:   objects.DTRow,
			PK:     mustDecodeHex(t, "00259da5fe4e202b974d64009944ccfe"),
			Sum:    mustDecodeHex(t, "13e5d3f474b88126a348e29ba12b4032"),
			OldSum: mustDecodeHex(t, "e4f37424a61671456b0be328e4f3719c"),
		},
		{
			Type: objects.DTRow,
			PK:   mustDecodeHex(t, "c5e86ba7d7653eec345ae9b6d77ab0cc"),
			Sum:  mustDecodeHex(t, "9937fd84049c00606a1b36ccf152168f"),
		},
		{
			Type:   objects.DTRow,
			PK:     mustDecodeHex(t, "e3c37d3bfd03aef8fac2794539e39160"),
			OldSum: mustDecodeHex(t, "1c51f6044190122c554cc6794585e654"),
		},
	})
}
