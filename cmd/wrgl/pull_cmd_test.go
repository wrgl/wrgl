package wrgl

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apitest "github.com/wrgl/core/pkg/api/test"
	confhelpers "github.com/wrgl/core/pkg/conf/helpers"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/ref"
)

func TestPullCmd(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := apitest.NewServer(t, nil)
	repo, url, _, cleanup := ts.NewRemote(t, true, "", nil)
	defer cleanup()
	dbs := ts.GetDB(repo)
	rss := ts.GetRS(repo)
	sum1, c1 := factory.CommitRandom(t, dbs, nil)
	sum2, c2 := factory.CommitRandom(t, dbs, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rss, "main", sum2, c2))
	sum4, c4 := factory.CommitRandom(t, dbs, nil)
	sum5, c5 := factory.CommitRandom(t, dbs, [][]byte{sum4})
	require.NoError(t, ref.CommitHead(rss, "beta", sum5, c5))

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	apitest.CopyCommitsToNewStore(t, dbs, db, [][]byte{sum1, sum4})
	require.NoError(t, ref.CommitHead(rs, "main", sum1, c1))
	require.NoError(t, ref.CommitHead(rs, "beta", sum4, c4))
	require.NoError(t, db.Close())

	cmd := RootCmd()
	cmd.SetArgs([]string{"remote", "add", "my-repo", url, "-t", "beta", "-t", "main"})
	require.NoError(t, cmd.Execute())

	cmd = RootCmd()
	cmd.SetArgs([]string{"pull", "main", "my-repo", "refs/heads/main:refs/remotes/my-repo/main", "--set-upstream"})
	assertCmdUnauthorized(t, cmd, url)
	authenticate(t, url)

	// pull set upstream
	cmd = RootCmd()
	cmd.SetArgs([]string{"pull", "main", "my-repo", "refs/heads/main:refs/remotes/my-repo/main", "--set-upstream"})
	cmd.SetOut(io.Discard)
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	sum, err := ref.GetHead(rs, "main")
	require.NoError(t, err)
	assert.Equal(t, sum2, sum)
	require.NoError(t, db.Close())

	sum3, c3 := factory.CommitRandom(t, dbs, [][]byte{sum2})
	require.NoError(t, ref.CommitHead(rss, "main", sum3, c3))

	// pull with upstream already set
	cmd = RootCmd()
	cmd.SetArgs([]string{"pull", "main"})
	cmd.SetOut(io.Discard)
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	sum, err = ref.GetHead(rs, "main")
	require.NoError(t, err)
	assert.Equal(t, sum3, sum)
	require.NoError(t, db.Close())

	// pull merge first fetch refspec
	cmd = RootCmd()
	cmd.SetArgs([]string{"pull", "beta", "my-repo"})
	cmd.SetOut(io.Discard)
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	sum, err = ref.GetHead(rs, "beta")
	require.NoError(t, err)
	assert.Equal(t, sum5, sum)
	require.NoError(t, db.Close())

	cmd = RootCmd()
	cmd.SetArgs([]string{"pull", "main"})
	assertCmdOutput(t, cmd, "Already up to date.\n")
}
