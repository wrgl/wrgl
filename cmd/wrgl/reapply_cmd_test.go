package wrgl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
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

func TestReapplyTxCmd(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()

	txid := startTransaction(t)

	header, fp := createRandomCSVFile(t)
	defer os.Remove(fp)
	cmd := rootCmd()
	cmd.SetArgs([]string{"commit", "alpha", fp, "first commit", "-p", header[0], "-n", "1", "--txid", txid})
	require.NoError(t, cmd.Execute())

	header, fp = createRandomCSVFile(t)
	defer os.Remove(fp)
	cmd = rootCmd()
	cmd.SetArgs([]string{"commit", "beta", fp, "first commit", "-p", header[0], "-n", "1", "--txid", txid})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"transaction", "commit", txid})
	require.NoError(t, cmd.Execute())

	header, fp = createRandomCSVFile(t)
	defer os.Remove(fp)
	commitFile(t, "alpha", fp, header[0])

	cmd = rootCmd()
	cmd.SetArgs([]string{"reapply", uuid.New().String()})
	assertCmdFailed(t, cmd, "", fmt.Errorf("transaction not found"))

	txid2 := startTransaction(t)
	cmd = rootCmd()
	cmd.SetArgs([]string{"reapply", txid2})
	assertCmdFailed(t, cmd, "", fmt.Errorf("transaction not committed"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"reapply", txid})
	buf := bytes.NewBuffer(nil)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())

	rs := rd.OpenRefStore()
	alphaSum, err := ref.GetHead(rs, "alpha")
	require.NoError(t, err)
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	alphaCom, err := objects.GetCommit(db, alphaSum)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	assert.True(t, strings.HasPrefix(alphaCom.Message, fmt.Sprintf("reapply [tx/%s]\n", txid)))

	output := buf.String()
	assert.True(t, strings.HasPrefix(output, fmt.Sprintf("Reapplying transaction %s\n", txid)))
	assert.Contains(t, output, fmt.Sprintf(
		"[alpha %s]\n    %s\n",
		hex.EncodeToString(alphaSum)[:7],
		strings.Replace(strings.TrimSpace(alphaCom.Message), "\n", "\n    ", -1),
	))
	assert.Contains(t, output, fmt.Sprintf("branch beta has not changed since\n"))
}

func TestReapplyCommitCmd(t *testing.T) {
	rd, cleanup := createRepoDir(t)
	defer cleanup()
	rs := rd.OpenRefStore()

	header, fp1 := createRandomCSVFile(t)
	defer os.Remove(fp1)
	commitFile(t, "alpha", fp1, header[0])
	sum1, err := ref.GetHead(rs, "alpha")
	require.NoError(t, err)
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	com1, err := objects.GetCommit(db, sum1)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	header, fp2 := createRandomCSVFile(t)
	defer os.Remove(fp2)
	commitFile(t, "alpha", fp2, header[0])
	sum2, err := ref.GetHead(rs, "alpha")
	require.NoError(t, err)

	cmd := rootCmd()
	cmd.SetArgs([]string{"reapply", hex.EncodeToString(sum2), "alpha"})
	assertCmdOutput(t, cmd, "branch alpha is already set to this commit\n")

	cmd = rootCmd()
	cmd.SetArgs([]string{"reapply", hex.EncodeToString(sum1), "alpha"})
	buf := bytes.NewBuffer(nil)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	sum3, err := ref.GetHead(rs, "alpha")
	require.NoError(t, err)
	assert.NotEqual(t, sum2, sum3)
	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	com3, err := objects.GetCommit(db, sum3)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	assert.True(t, strings.HasPrefix(com3.Message, fmt.Sprintf("reapply [com/%x]\n", sum1)))
	assert.Equal(t, [][]byte{sum2}, com3.Parents)
	assert.Equal(t, com1.Table, com3.Table)

	assert.Equal(t, fmt.Sprintf(
		"[alpha %s]\n    %s\n\n",
		hex.EncodeToString(sum3)[:7],
		strings.Replace(strings.TrimSpace(com3.Message), "\n", "\n    ", -1),
	), buf.String())
}
