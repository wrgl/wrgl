package refsql

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/ref"
	refhelpers "github.com/wrgl/wrgl/pkg/ref/helpers"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestStore(t *testing.T) {
	db, cleanup := testutils.CreateSQLDB(t, CreateTableStmts)
	defer cleanup()
	s := NewStore(db)

	sum := testutils.SecureRandomBytes(16)
	require.NoError(t, s.Set("heads/alpha", sum))
	v, err := s.Get("heads/alpha")
	require.NoError(t, err)
	assert.Equal(t, sum, v)

	rl1 := refhelpers.RandomReflog()
	rl1.OldOID = nil
	require.NoError(t, s.SetWithLog("heads/beta", rl1.NewOID, rl1))
	rl2 := refhelpers.RandomReflog()
	rl2.OldOID = rl1.NewOID
	require.NoError(t, s.SetWithLog("heads/beta", rl2.NewOID, rl2))
	v, err = s.Get("heads/beta")
	require.NoError(t, err)
	assert.Equal(t, rl2.NewOID, v)

	refhelpers.AssertReflogReaderContains(t, s, "heads/beta", rl2, rl1)

	require.NoError(t, s.Copy("heads/beta", "heads/beta-1"))
	v, err = s.Get("heads/beta-1")
	require.NoError(t, err)
	assert.Equal(t, rl2.NewOID, v)
	v, err = s.Get("heads/beta")
	require.NoError(t, err)
	assert.Equal(t, rl2.NewOID, v)
	refhelpers.AssertReflogReaderContains(t, s, "heads/beta", rl2, rl1)
	refhelpers.AssertReflogReaderContains(t, s, "heads/beta-1", rl2, rl1)

	require.NoError(t, s.Rename("heads/beta", "heads/beta-2"))
	v, err = s.Get("heads/beta-2")
	require.NoError(t, err)
	assert.Equal(t, rl2.NewOID, v)
	_, err = s.Get("heads/beta")
	assert.Equal(t, ref.ErrKeyNotFound, err)
	refhelpers.AssertReflogReaderContains(t, s, "heads/beta-2", rl2, rl1)
	_, err = s.LogReader("heads/beta")
	assert.Error(t, err)

	require.NoError(t, s.Set("tags/dec-2022", testutils.SecureRandomBytes(16)))
	for i, c := range []struct {
		Prefixes    []string
		NotPrefixes []string
		Expected    []string
	}{
		{nil, nil, []string{"heads/alpha", "heads/beta-1", "heads/beta-2", "tags/dec-2022"}},
		{[]string{"heads/"}, nil, []string{"heads/alpha", "heads/beta-1", "heads/beta-2"}},
		{[]string{"heads/a", "heads/beta-1"}, nil, []string{"heads/alpha", "heads/beta-1"}},
		{nil, []string{"heads/"}, []string{"tags/dec-2022"}},
		{[]string{"heads/"}, []string{"heads/alpha"}, []string{"heads/beta-1", "heads/beta-2"}},
		{[]string{"heads/", "tags/"}, []string{"heads/alpha"}, []string{"heads/beta-1", "heads/beta-2", "tags/dec-2022"}},
	} {
		keys, err := s.FilterKey(c.Prefixes, c.NotPrefixes)
		require.NoError(t, err)
		assert.Equal(t, c.Expected, keys, "case %d", i)
		m, err := s.Filter(c.Prefixes, c.NotPrefixes)
		require.NoError(t, err)
		assert.Len(t, m, len(c.Expected), "case %d", i)
		for _, r := range c.Expected {
			assert.Contains(t, m, r, "case %d", i)
		}
	}

	m, err := s.Filter([]string{"heads/"}, nil)
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"heads/alpha":  sum,
		"heads/beta-1": rl2.NewOID,
		"heads/beta-2": rl2.NewOID,
	}, m)
	m, err = s.Filter([]string{"heads/b"}, nil)
	require.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"heads/beta-1": rl2.NewOID,
		"heads/beta-2": rl2.NewOID,
	}, m)

	require.NoError(t, s.Delete("heads/alpha"))
	_, err = s.Get("heads/alpha")
	assert.Equal(t, ref.ErrKeyNotFound, err)

	require.NoError(t, s.Delete("heads/beta-1"))
	_, err = s.Get("heads/beta-1")
	assert.Equal(t, ref.ErrKeyNotFound, err)
	_, err = s.LogReader("heads/beta-1")
	assert.Error(t, err)
}

func TestCreateTransaction(t *testing.T) {
	db, cleanup := testutils.CreateSQLDB(t, CreateTableStmts)
	defer cleanup()
	s := NewStore(db)

	txid, err := s.NewTransaction(nil)
	require.NoError(t, err)

	tx, err := s.GetTransaction(*txid)
	require.NoError(t, err)
	assert.Equal(t, tx.ID, *txid)
	assert.NotEmpty(t, tx.Begin)
	assert.Equal(t, ref.TSInProgress, tx.Status)
	assert.Empty(t, tx.End)

	tx.End = time.Now()
	tx.Status = ref.TSCommitted
	require.NoError(t, s.UpdateTransaction(tx))
	tx2, err := s.GetTransaction(*txid)
	require.NoError(t, err)
	refhelpers.AssertTransactionEqual(t, tx, tx2)

	txid2, err := s.NewTransaction(nil)
	require.NoError(t, err)
	require.NoError(t, s.DeleteTransaction(*txid2))

	txid3, err := s.NewTransaction(nil)
	require.NoError(t, err)
	txid4, err := s.NewTransaction(nil)
	require.NoError(t, err)

	time.Sleep(time.Second)
	txid5, err := s.NewTransaction(nil)
	require.NoError(t, err)
	ids, err := s.GCTransactions(time.Second)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{*txid3, *txid4}, ids)
	_, err = s.GetTransaction(*txid3)
	assert.Error(t, err)
	_, err = s.GetTransaction(*txid4)
	assert.Error(t, err)
	_, err = s.GetTransaction(*txid5)
	require.NoError(t, err)

	assert.Equal(t, fmt.Errorf("cannot discard committed transaction"), s.DeleteTransaction(*txid))
}

func TestTransactionLog(t *testing.T) {
	db, cleanup := testutils.CreateSQLDB(t, CreateTableStmts)
	defer cleanup()
	s := NewStore(db)

	txid, err := s.NewTransaction(nil)
	require.NoError(t, err)

	rl1 := refhelpers.RandomReflog()
	rl1.OldOID = nil
	require.NoError(t, s.SetWithLog("heads/alpha", rl1.NewOID, rl1))

	rl2 := refhelpers.RandomReflog()
	rl2.OldOID = rl1.NewOID
	rl2.Txid = txid
	require.NoError(t, s.SetWithLog("heads/alpha", rl2.NewOID, rl2))

	refhelpers.AssertLatestReflogEqual(t, s, "heads/alpha", rl2)

	rl3 := refhelpers.RandomReflog()
	rl3.OldOID = nil
	rl3.Txid = txid
	require.NoError(t, s.SetWithLog("heads/beta", rl3.NewOID, rl3))

	m, err := s.GetTransactionLogs(*txid)
	require.NoError(t, err)
	assert.Len(t, m, 2)
	refhelpers.AssertReflogEqual(t, rl2, m["heads/alpha"])
	refhelpers.AssertReflogEqual(t, rl3, m["heads/beta"])
}

func TestListTransactions(t *testing.T) {
	db, cleanup := testutils.CreateSQLDB(t, CreateTableStmts)
	defer cleanup()
	s := NewStore(db)

	txids := []*uuid.UUID{}
	for i := 0; i < 4; i++ {
		txid, err := s.NewTransaction(nil)
		require.NoError(t, err)
		txids = append(txids, txid)
	}

	txs := make([]*ref.Transaction, len(txids))
	for i, txid := range txids {
		tx, err := s.GetTransaction(*txid)
		require.NoError(t, err)
		txs[i] = tx
	}

	txs[0].Status = ref.TSCommitted
	txs[0].End = time.Now()
	require.NoError(t, s.UpdateTransaction(txs[0]))

	sl, err := s.ListTransactions(0, 10)
	require.NoError(t, err)
	refhelpers.AssertTransactionSliceEqual(t, []*ref.Transaction{
		txs[3], txs[2], txs[1], txs[0],
	}, sl)

	sl, err = s.ListTransactions(1, 2)
	require.NoError(t, err)
	refhelpers.AssertTransactionSliceEqual(t, []*ref.Transaction{txs[2], txs[1]}, sl)
}

func TestCreateTransactionFromExisting(t *testing.T) {
	db, cleanup := testutils.CreateSQLDB(t, CreateTableStmts)
	defer cleanup()
	s := NewStore(db)

	tx := &ref.Transaction{
		ID:     uuid.New(),
		Begin:  time.Now().Add(-time.Hour),
		Status: ref.TSInProgress,
		End:    time.Now(),
	}
	txid, err := s.NewTransaction(tx)
	require.NoError(t, err)
	assert.Equal(t, tx.ID, *txid)
	tx2, err := s.GetTransaction(*txid)
	require.NoError(t, err)
	refhelpers.AssertTransactionEqual(t, tx, tx2)
}
