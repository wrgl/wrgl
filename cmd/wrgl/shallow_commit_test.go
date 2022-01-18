package wrgl

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apitest "github.com/wrgl/wrgl/pkg/api/test"
	confhelpers "github.com/wrgl/wrgl/pkg/conf/helpers"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/ref"
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
	assertCmdFailed(t, cmd, "", fmt.Errorf("GetTable: table %x not found, try fetching it with:\n  wrgl fetch tables my-repo %x", c1.Table, c1.Table))

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
}
