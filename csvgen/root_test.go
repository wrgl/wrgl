// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"bytes"
	"encoding/csv"
	"io/ioutil"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createRandomCSV(t *testing.T, n, m int) (string, [][]string, func()) {
	t.Helper()
	f, err := ioutil.TempFile("", "*.csv")
	require.NoError(t, err)
	w := csv.NewWriter(f)
	rows := make([][]string, n)
	for i := 0; i < n; i++ {
		row := make([]string, m)
		for j := 0; j < m; j++ {
			row[j] = brokenRandomAlphaNumericString(5)
		}
		require.NoError(t, w.Write(row))
		rows[i] = row
	}
	w.Flush()
	require.NoError(t, f.Close())
	return f.Name(), rows, func() {
		require.NoError(t, os.Remove(f.Name()))
	}
}

func readCMDOutput(t *testing.T, cmd *cobra.Command) [][]string {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	res, err := csv.NewReader(buf).ReadAll()
	require.NoError(t, err)
	return res
}

func TestRootCmdTooFewColumns(t *testing.T) {
	name, _, cleanup := createRandomCSV(t, 5, 3)
	defer cleanup()

	cmd := newRootCmd()
	cmd.SetArgs([]string{name})
	err := cmd.Execute()
	assert.Equal(t, "original file has too few columns, try to pass in file with minimum 5 columns", err.Error())
}

func TestRootCmd(t *testing.T) {
	name, rows, cleanup := createRandomCSV(t, 21, 10)
	defer cleanup()

	cmd := newRootCmd()
	cmd.SetArgs([]string{name})
	res := readCMDOutput(t, cmd)
	assert.Len(t, res, 21)
	assert.Equal(t, []string{
		"col_a", "col_b", "col_c", "col_d", "col_e", "col_f", "col_g", "col_h", "col_i", "col_j",
	}, res[0])
	assert.NotEqual(t, rows[0], res[0])
	assert.Equal(t, rows[1:], res[1:])

	cmd = newRootCmd()
	cmd.SetArgs([]string{name, "--addrem-cols"})
	res = readCMDOutput(t, cmd)
	assert.Len(t, res, 21)
	assert.NotEqual(t, []string{
		"col_a", "col_b", "col_c", "col_d", "col_e", "col_f", "col_g", "col_h", "col_i", "col_j",
	}, res[0])
	assert.NotEqual(t, rows[1:], res[1:])

	cmd = newRootCmd()
	cmd.SetArgs([]string{name, "--rename-cols"})
	res = readCMDOutput(t, cmd)
	assert.Len(t, res, 21)
	assert.NotEqual(t, []string{
		"col_a", "col_b", "col_c", "col_d", "col_e", "col_f", "col_g", "col_h", "col_i", "col_j",
	}, res[0])
	assert.Equal(t, rows[1:], res[1:])

	cmd = newRootCmd()
	cmd.SetArgs([]string{name, "--move-cols"})
	res = readCMDOutput(t, cmd)
	assert.Len(t, res, 21)
	assert.NotEqual(t, []string{
		"col_a", "col_b", "col_c", "col_d", "col_e", "col_f", "col_g", "col_h", "col_i", "col_j",
	}, res[0])
	assert.NotEqual(t, rows[1:], res[1:])

	cmd = newRootCmd()
	cmd.SetArgs([]string{name, "--mod-rows"})
	res = readCMDOutput(t, cmd)
	assert.Len(t, res, 21)
	assert.Equal(t, []string{
		"col_a", "col_b", "col_c", "col_d", "col_e", "col_f", "col_g", "col_h", "col_i", "col_j",
	}, res[0])
	assert.NotEqual(t, rows[1:], res[1:])
}
