package wrgl

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/objects"
)

func TestGCCmd(t *testing.T) {
	rd, cleanUp := createRepoDir(t)
	defer cleanUp()
	db, err := rd.OpenObjectsStore()
	require.NoError(t, err)
	sum, _ := factory.CommitRandom(t, db, nil)
	require.NoError(t, db.Close())

	cmd := rootCmd()
	cmd.SetArgs([]string{"config", "set", "transactionTTL", "1s"})
	require.NoError(t, cmd.Execute())
	txid := startTransaction(t)

	time.Sleep(time.Second)

	cmd = rootCmd()
	cmd.SetArgs([]string{"gc"})
	require.NoError(t, cmd.Execute())

	db, err = rd.OpenObjectsStore()
	require.NoError(t, err)
	assert.False(t, objects.CommitExist(db, sum))
	tid, err := uuid.Parse(txid)
	require.NoError(t, err)
	assert.False(t, objects.TransactionExist(db, tid))
	require.NoError(t, db.Close())
}
