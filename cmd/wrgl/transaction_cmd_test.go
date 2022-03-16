// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func createRandomCSVFile(t *testing.T) (header []string, filePath string) {
	t.Helper()
	rows := testutils.BuildRawCSV(3, 4)
	content := make([]string, len(rows))
	for i, row := range rows {
		content[i] = strings.Join(row, ",")
	}
	return createCSVFile(t, content)
}

func getRefs(t *testing.T, rs ref.Store, refs ...string) (sums map[string][]byte) {
	t.Helper()
	sums = map[string][]byte{}
	for _, r := range refs {
		sum, err := ref.GetRef(rs, r)
		if err == nil {
			sums[r] = sum
		}
	}
	return
}

func startTransaction(t *testing.T) string {
	cmd := rootCmd()
	cmd.SetArgs([]string{"transaction", "start"})
	buf := bytes.NewBuffer(nil)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	return strings.TrimSpace(buf.String())
}

func TestTransactionCmd(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()
	rs := rd.OpenRefStore()

	txid := startTransaction(t)

	_, fp := createCSVFile(t, []string{
		"a,f,g",
		"1,t,y",
		"2,g,h",
		"3,b,n",
	})
	defer os.Remove(fp)
	commitFile(t, "alpha", fp, "a")
	_, fp = createCSVFile(t, []string{
		"a,f,g",
		"1,t,r",
		"2,g,f",
		"3,b,v",
	})
	defer os.Remove(fp)
	cmd := rootCmd()
	cmd.SetArgs([]string{"commit", "alpha", fp, "second commit", "-p", "a", "-n", "1", "--txid", txid})
	require.NoError(t, cmd.Execute())
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	alphaTxSum, err := ref.GetRef(rs, ref.TransactionRef(txid, "alpha"))
	require.NoError(t, err)
	alphaSum, err := ref.GetHead(rs, "alpha")
	require.NoError(t, err)
	alphaTxCom, err := objects.GetCommit(db, alphaTxSum)
	require.NoError(t, err)
	alphaCom, err := objects.GetCommit(db, alphaSum)
	require.NoError(t, err)
	require.NotEqual(t, alphaTxCom.Table, alphaCom.Table)
	require.NoError(t, db.Close())

	files := map[string]string{}
	for _, branch := range []string{"beta", "gamma", "delta"} {
		header, fp := createRandomCSVFile(t)
		defer os.Remove(fp)
		files[branch] = fp
		cmd = rootCmd()
		cmd.SetArgs([]string{"config", "set", fmt.Sprintf("branch.%s.file", branch), fp})
		require.NoError(t, cmd.Execute())
		cmd = rootCmd()
		cmd.SetArgs([]string{"config", "set", fmt.Sprintf("branch.%s.primaryKey", branch), fmt.Sprintf(`[%q]`, header[0])})
	}

	cmd = rootCmd()
	cmd.SetArgs([]string{"commit", "beta", files["beta"], "initial commit", "-n", "1", "--txid", txid})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"commit", "--all", "initial commit", "-n", "1", "--txid", txid})
	require.NoError(t, cmd.Execute())

	m, _ := rs.Filter("")
	for k, sum := range m {
		t.Logf("ref %s: %x", k, sum)
	}
	txSums := getRefs(t, rs,
		ref.TransactionRef(txid, "alpha"),
		ref.TransactionRef(txid, "beta"),
		ref.TransactionRef(txid, "gamma"),
		ref.TransactionRef(txid, "delta"),
	)
	require.Len(t, txSums, 4)
	cmd = rootCmd()
	cmd.SetArgs([]string{"diff", "--txid", txid})
	buf := bytes.NewBuffer(nil)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	outStr := removeColor(buf.String())
	for _, line := range []string{
		"Changes from transaction " + txid,
		`Branch "beta" didn't previously exist, skipping.`,
		`Branch "delta" didn't previously exist, skipping.`,
		`Branch "gamma" didn't previously exist, skipping.`,
		"alpha rows: m3",
	} {
		assert.Contains(t, outStr, line+"\n")
	}

	cmd = rootCmd()
	cmd.SetArgs([]string{"transaction", "commit", txid})
	require.NoError(t, cmd.Execute())

	sums := getRefs(t, rs,
		ref.HeadRef("alpha"),
		ref.HeadRef("beta"),
		ref.HeadRef("gamma"),
		ref.HeadRef("delta"),
	)
	assert.Equal(t, txSums[ref.TransactionRef(txid, "beta")], sums[ref.HeadRef("beta")])
	assert.Equal(t, txSums[ref.TransactionRef(txid, "gamma")], sums[ref.HeadRef("gamma")])
	assert.Equal(t, txSums[ref.TransactionRef(txid, "delta")], sums[ref.HeadRef("delta")])
	assert.Equal(t, txSums[ref.TransactionRef(txid, "alpha")], sums[ref.HeadRef("alpha")])
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	alphaCom, err = objects.GetCommit(db, sums[ref.HeadRef("alpha")])
	assert.Equal(t, [][]byte{alphaSum}, alphaCom.Parents)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	txSums = getRefs(t, rs,
		ref.TransactionRef(txid, "alpha"),
		ref.TransactionRef(txid, "beta"),
		ref.TransactionRef(txid, "gamma"),
		ref.TransactionRef(txid, "delta"),
	)
	assert.Len(t, txSums, 0)
}

func TestTransactionDiscardCmd(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()
	rs := rd.OpenRefStore()

	txid := startTransaction(t)

	header, fp := createRandomCSVFile(t)
	defer os.Remove(fp)
	cmd := rootCmd()
	cmd.SetArgs([]string{"commit", "alpha", fp, "initial commit", "-n", "1", "--txid", txid, "-p", header[0]})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"transaction", "discard", txid})
	require.NoError(t, cmd.Execute())

	txSums := getRefs(t, rs,
		ref.TransactionRef(txid, "alpha"),
		ref.HeadRef("alpha"),
	)
	assert.Len(t, txSums, 0)
}
