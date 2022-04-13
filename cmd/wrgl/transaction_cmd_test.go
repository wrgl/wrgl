// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package wrgl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	confhelpers "github.com/wrgl/wrgl/pkg/conf/helpers"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/testutils"
	server_testutils "github.com/wrgl/wrgl/wrgld/pkg/server/testutils"
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
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	for _, branch := range []string{"alpha", "beta", "gamma", "delta"} {
		txCom, err := objects.GetCommit(db, txSums[ref.TransactionRef(txid, branch)])
		require.NoError(t, err)
		com, err := objects.GetCommit(db, sums[ref.HeadRef(branch)])
		require.NoError(t, err)
		assert.NotEqual(t, txCom.Sum, com.Sum)
		assert.Equal(t, fmt.Sprintf("commit [tx/%s]\n%s", txid, txCom.Message), com.Message)
		assert.Equal(t, txCom.Table, com.Table)
		if branch == "alpha" {
			assert.Equal(t, [][]byte{alphaSum}, com.Parents)
		} else {
			assert.Empty(t, com.Parents)
		}
	}
	require.NoError(t, db.Close())
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

func TestTransactionListCmd(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := server_testutils.NewServer(t, nil)
	defer ts.Close()
	repo, url, _, cleanup := ts.NewRemote(t, "", nil)
	defer cleanup()

	rd, cleanup := createRepoDir(t)
	defer cleanup()

	txid1 := startTransaction(t)
	txid2 := startTransaction(t)

	cmd := rootCmd()
	cmd.SetArgs([]string{"remote", "add", "my-repo", url})
	require.NoError(t, cmd.Execute())
	authenticate(t, ts, url)

	cmd = rootCmd()
	cmd.SetArgs([]string{"transaction", "push", "my-repo", txid1})
	assertCmdFailed(t, cmd, "", fmt.Errorf("transaction is empty"))

	header, fp := createRandomCSVFile(t)
	defer os.Remove(fp)
	cmd = rootCmd()
	cmd.SetArgs([]string{"commit", "alpha", fp, "initial commit", "-n", "1", "--txid", txid1, "-p", header[0]})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"transaction", "commit", txid1})
	require.NoError(t, cmd.Execute())

	rs := rd.OpenRefStore()
	id1 := uuid.Must(uuid.Parse(txid1))
	tx1, err := rs.GetTransaction(id1)
	require.NoError(t, err)
	id2 := uuid.Must(uuid.Parse(txid2))
	tx2, err := rs.GetTransaction(id2)
	require.NoError(t, err)
	alphaSum, err := ref.GetRef(rs, ref.TransactionRef(txid1, "alpha"))
	require.NoError(t, err)

	cmd = rootCmd()
	zone, offset := time.Now().Zone()
	cmd.SetArgs([]string{"transaction", "list", "--no-pager"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("transaction %s", txid2),
		"Status: in-progress",
		fmt.Sprintf("Begin: %s", tx2.Begin.In(time.FixedZone(zone, offset))),
		"",
		"",
		fmt.Sprintf("transaction %s", txid1),
		"Status: committed",
		fmt.Sprintf("Begin: %s", tx1.Begin.In(time.FixedZone(zone, offset))),
		fmt.Sprintf("End: %s", tx1.End.In(time.FixedZone(zone, offset))),
		"",
		fmt.Sprintf("    [alpha %s] initial commit", hex.EncodeToString(alphaSum)[:7]),
		"",
		"",
	}, "\n"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"transaction", "push", "--no-progress", "my-repo", txid1})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"To " + url,
		fmt.Sprintf("transaction %s created", txid1),
		fmt.Sprintf(" * [new reference]   refs/txs/%s/alpha -> refs/txs/%s/alpha", txid1, txid1),
		"",
	}, "\n"))

	rss := ts.GetRS(repo)
	sum, err := ref.GetRef(rss, ref.TransactionRef(txid1, "alpha"))
	require.NoError(t, err)
	assert.Equal(t, alphaSum, sum)
	tx3, err := rss.GetTransaction(id1)
	require.NoError(t, err)
	assert.Equal(t, tx1, tx3)
}
