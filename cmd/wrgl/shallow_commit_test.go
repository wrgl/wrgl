package wrgl

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	apitest "github.com/wrgl/wrgl/pkg/api/test"
	confhelpers "github.com/wrgl/wrgl/pkg/conf/helpers"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
)

func TestShallowCommit(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := apitest.NewServer(t, nil)
	repo, url, _, cleanup := ts.NewRemote(t, true, "", nil)
	defer cleanup()
	dbs := ts.GetDB(repo)
	rss := ts.GetRS(repo)
	sum1, c1 := factory.CommitRandom(t, dbs, nil)
	sum2, c2 := factory.CommitRandom(t, dbs, [][]byte{sum1})
	sum3, c3 := factory.CommitRandom(t, dbs, [][]byte{sum2})
	require.NoError(t, ref.CommitHead(rss, "main", sum3, c3))

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	cmd := rootCmd()
	cmd.SetArgs([]string{"remote", "add", "my-repo", url})
	require.NoError(t, cmd.Execute())

	// test pull depth
	authenticate(t, url)
	cmd = rootCmd()
	cmd.SetArgs([]string{"pull", "main", "my-repo", "refs/heads/main:refs/remotes/my-repo/main", "--depth", "1"})
	require.NoError(t, cmd.Execute())

	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	apitest.AssertCommitsShallowlyPersisted(t, db, [][]byte{sum1, sum2, sum3})
	apitest.AssertTablePersisted(t, db, c3.Table)
	apitest.AssertTablesNotPersisted(t, db, [][]byte{c1.Table, c2.Table})
	rs := rd.OpenRefStore()
	sum, err := ref.GetHead(rs, "main")
	require.NoError(t, err)
	assert.Equal(t, sum3, sum)
	require.NoError(t, db.Close())

	// test preview shallow commit
	cmd = rootCmd()
	cmd.SetArgs([]string{"preview", hex.EncodeToString(sum1)})
	assertCmdFailed(t, cmd, "", fmt.Errorf("table %x not found, try fetching it with:\n  wrgl fetch tables my-repo %x", c1.Table, c1.Table))

	cmd = rootCmd()
	cmd.SetArgs([]string{"export", hex.EncodeToString(sum1)})
	assertCmdFailed(t, cmd, "", fmt.Errorf("table %x not found, try fetching it with:\n  wrgl fetch tables my-repo %x", c1.Table, c1.Table))

	// test log shallow commit
	cmd = rootCmd()
	cmd.SetArgs([]string{"log", "main", "--no-pager"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("commit %x", sum3),
		fmt.Sprintf("table %x", c3.Table),
		fmt.Sprintf("Author: %s <%s>", c3.AuthorName, c3.AuthorEmail),
		fmt.Sprintf("Date: %s", c3.Time.In(time.FixedZone("+0700", 7*3600)).Truncate(time.Second)),
		"",
		fmt.Sprintf("    %s", c3.Message),
		"",
		fmt.Sprintf("commit %x", sum2),
		fmt.Sprintf("table %x <missing, possibly reside on my-repo>", c2.Table),
		fmt.Sprintf("Author: %s <%s>", c2.AuthorName, c2.AuthorEmail),
		fmt.Sprintf("Date: %s", c2.Time.In(time.FixedZone("+0700", 7*3600)).Truncate(time.Second)),
		"",
		fmt.Sprintf("    %s", c2.Message),
		"",
		fmt.Sprintf("commit %x", sum1),
		fmt.Sprintf("table %x <missing, possibly reside on my-repo>", c1.Table),
		fmt.Sprintf("Author: %s <%s>", c1.AuthorName, c1.AuthorEmail),
		fmt.Sprintf("Date: %s", c1.Time.In(time.FixedZone("+0700", 7*3600)).Truncate(time.Second)),
		"",
		fmt.Sprintf("    %s", c1.Message),
		"",
		"",
	}, "\n"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"profile", hex.EncodeToString(sum1)})
	assertCmdFailed(t, cmd, "", fmt.Errorf("table %x not found, try fetching it with:\n  wrgl fetch tables my-repo %x", c1.Table, c1.Table))

	cmd = rootCmd()
	cmd.SetArgs([]string{"diff", "main"})
	assertCmdFailed(t, cmd, "", fmt.Errorf("table %x not found, try fetching it with:\n  wrgl fetch tables my-repo %x", c2.Table, c2.Table))

	cmd = rootCmd()
	cmd.SetArgs([]string{"diff", hex.EncodeToString(sum2), hex.EncodeToString(sum1)})
	assertCmdFailed(t, cmd, "", fmt.Errorf("table %x not found, try fetching it with:\n  wrgl fetch tables my-repo %x", c2.Table, c2.Table))

	cmd = rootCmd()
	cmd.SetArgs([]string{"reset", "main", hex.EncodeToString(sum1)})
	assertCmdFailed(t, cmd, "", fmt.Errorf("cannot reset branch to a shallow commit: table %x is missing. Fetch missing table with:\n  wrgl fetch tables my-repo %x", c1.Table, c1.Table))

	cmd = rootCmd()
	cmd.SetArgs([]string{"cat-obj", hex.EncodeToString(sum1)})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("\x1b[33mtable\x1b[97m  %x\x1b[0m \x1b[31m<missing, possibly reside on my-repo>\x1b[97m\x1b[0m", c1.Table),
		fmt.Sprintf("\x1b[33mauthor\x1b[97m %s <%s>", c1.AuthorName, c1.AuthorEmail),
		fmt.Sprintf("\x1b[0m\x1b[33mtime\x1b[97m   %d %s", c1.Time.Unix(), c1.Time.Format("-0700")),
		"",
		fmt.Sprintf("\x1b[0m%s", c1.Message),
		"",
	}, "\n"))

	cmd = rootCmd()
	cmd.SetArgs([]string{"cat-obj", hex.EncodeToString(c1.Table)})
	assertCmdFailed(t, cmd, "", fmt.Errorf("unrecognized hash"))

	require.NoError(t, ref.DeleteHead(rs, "main"))
	require.NoError(t, ref.DeleteRemoteRef(rs, "my-repo", "main"))
	cmd = rootCmd()
	cmd.SetArgs([]string{"prune"})
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	sl, err := objects.GetAllCommitKeys(db)
	require.NoError(t, err)
	assert.Len(t, sl, 0)
	sl, err = objects.GetAllTableKeys(db)
	require.NoError(t, err)
	assert.Len(t, sl, 0)
	require.NoError(t, db.Close())

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	sum4, c4 := refhelpers.SaveTestCommit(t, db, nil)
	sum5, c5 := factory.CommitRandom(t, db, [][]byte{sum4})
	sum6, c6 := refhelpers.SaveTestCommit(t, db, [][]byte{sum5})
	require.NoError(t, ref.CommitHead(rs, "alpha", sum5, c5))
	require.NoError(t, db.Close())

	cmd = rootCmd()
	cmd.SetArgs([]string{"push", "my-repo", "refs/heads/alpha:alpha"})
	assertCmdFailed(t, cmd, fmt.Sprintf("To %s\n", url), apiclient.NewShallowCommitError(sum4, c4.Table))

	cmd = rootCmd()
	cmd.SetArgs([]string{"merge", "alpha", hex.EncodeToString(sum6)})
	assertCmdFailed(t, cmd, "", fmt.Errorf("table %x not found", c6.Table))
}
