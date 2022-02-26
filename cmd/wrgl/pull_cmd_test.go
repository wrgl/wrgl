package wrgl

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apitest "github.com/wrgl/wrgl/pkg/api/test"
	"github.com/wrgl/wrgl/pkg/auth"
	confhelpers "github.com/wrgl/wrgl/pkg/conf/helpers"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/ref"
)

func TestPullCmd(t *testing.T) {
	defer confhelpers.MockGlobalConf(t, true)()
	ts := apitest.NewServer(t, nil)
	repo, url, _, cleanup := ts.NewRemote(t, true, "", nil)
	defer cleanup()
	dbs := ts.GetDB(repo)
	rss := ts.GetRS(repo)
	sum1, _ := factory.CommitRandom(t, dbs, nil)
	sum2, c2 := factory.CommitRandom(t, dbs, [][]byte{sum1})
	require.NoError(t, ref.CommitHead(rss, "main", sum2, c2))
	sum4, c4 := factory.CommitRandom(t, dbs, nil)
	sum5, c5 := factory.CommitRandom(t, dbs, [][]byte{sum4})
	require.NoError(t, ref.CommitHead(rss, "beta", sum5, c5))
	sum6, c6 := factory.CommitRandom(t, dbs, nil)
	require.NoError(t, ref.CommitHead(rss, "gamma", sum6, c6))

	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	rs := rd.OpenRefStore()
	apitest.CopyCommitsToNewStore(t, dbs, db, [][]byte{sum1, sum4})
	require.NoError(t, ref.CommitHead(rs, "beta", sum4, c4))
	require.NoError(t, db.Close())

	cmd := rootCmd()
	cmd.SetArgs([]string{"remote", "add", "my-repo", url})
	require.NoError(t, cmd.Execute())

	cmd = rootCmd()
	cmd.SetArgs([]string{"pull", "main", "my-repo", "refs/heads/main:refs/remotes/my-repo/main", "--set-upstream"})
	assertCmdUnauthorized(t, cmd, url)

	// pull set upstream
	authenticate(t, url)
	cmd = rootCmd()
	cmd.SetArgs([]string{"pull", "main", "my-repo", "refs/heads/main:refs/remotes/my-repo/main", "--set-upstream"})
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
	cmd = rootCmd()
	cmd.SetArgs([]string{"pull", "main"})
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	sum, err = ref.GetHead(rs, "main")
	require.NoError(t, err)
	assert.Equal(t, sum3, sum)
	require.NoError(t, db.Close())

	// pull merge first fetch refspec
	cmd = rootCmd()
	cmd.SetArgs([]string{"pull", "beta", "my-repo"})
	assertCmdOutput(t, cmd, "Already up to date.\n")

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	sum, err = ref.GetHead(rs, "beta")
	require.NoError(t, err)
	assert.Equal(t, sum4, sum)
	require.NoError(t, db.Close())

	cmd = rootCmd()
	cmd.SetArgs([]string{"pull", "main"})
	assertCmdOutput(t, cmd, "Already up to date.\n")

	// configure gamma upstream
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "set", "branch.gamma.remote", "my-repo"})
	require.NoError(t, cmd.Execute())
	cmd = rootCmd()
	cmd.SetArgs([]string{"config", "set", "branch.gamma.merge", "refs/heads/gamma"})
	require.NoError(t, cmd.Execute())

	// pull all branches with upstream configured
	cmd = rootCmd()
	cmd.SetArgs([]string{"pull", "--all", "--no-progress"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		"pulling \x1b[1mgamma\x1b[0m...",
		fmt.Sprintf("\x1b[0m[gamma %s] %s", hex.EncodeToString(sum6)[:7], c6.Message),
		"pulling \x1b[1mmain\x1b[0m...",
		"\x1b[0mAlready up to date.",
		"",
	}, "\n"))
	sum, err = ref.GetHead(rs, "gamma")
	require.NoError(t, err)
	assert.Equal(t, sum6, sum)

	// pull from public repo as an anynomous user
	sum7, c7 := factory.CommitRandom(t, dbs, [][]byte{sum3})
	require.NoError(t, ref.CommitHead(rss, "main", sum7, c7))
	unauthenticate(t, url)
	cmd = rootCmd()
	cmd.SetArgs([]string{"pull", "main"})
	assertCmdUnauthorized(t, cmd, url)
	authzS := ts.GetAuthzS(repo)
	require.NoError(t, authzS.AddPolicy(auth.Anyone, auth.ScopeRepoRead))
	cmd = rootCmd()
	cmd.SetArgs([]string{"pull", "main", "--no-progress"})
	assertCmdOutput(t, cmd, strings.Join([]string{
		fmt.Sprintf("No credential found for %s", url),
		"Proceed as anonymous user...",
		fmt.Sprintf("From %s", url),
		fmt.Sprintf("   %s..%s  main        -> my-repo/main", hex.EncodeToString(sum3)[:7], hex.EncodeToString(sum7)[:7]),
		fmt.Sprintf("Fast forward to %s", hex.EncodeToString(sum7)[:7]),
		"",
	}, "\n"))
	sum, err = ref.GetHead(rs, "main")
	require.NoError(t, err)
	assert.Equal(t, sum7, sum)
}
