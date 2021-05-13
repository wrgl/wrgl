package pack

import (
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/mmcloughlin/meow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	packclient "github.com/wrgl/core/pkg/pack/client"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/pkg/versioning"
)

var tg func() time.Time

func init() {
	tg = testutils.CreateTimeGen()
}

func createCommit(t *testing.T, db kv.DB, fs kv.FileStore, parents [][]byte) (sum []byte, commit *objects.Commit) {
	t.Helper()
	rows := testutils.BuildRawCSV(4, 4)
	b := table.NewBuilder(db, fs, rows[0], []uint32{0}, 0, 0)
	rh := ingest.NewRowHasher([]uint32{0}, 0)
	for i, row := range rows[1:] {
		keySum, rowSum, rowContent, err := rh.Sum(row)
		require.NoError(t, err)
		err = b.InsertRow(i, keySum, rowSum, rowContent)
		require.NoError(t, err)
	}
	tSum, err := b.SaveTable()
	require.NoError(t, err)
	commit = &objects.Commit{
		Table:       tSum,
		AuthorName:  "John Doe",
		AuthorEmail: "john@doe.com",
		Time:        tg(),
		Message:     testutils.BrokenRandomAlphaNumericString(40),
		Parents:     parents,
	}
	sum, err = versioning.SaveCommit(db, 0, commit)
	require.NoError(t, err)
	return
}

func assertSentMissingCommits(t *testing.T, db kv.DB, fs kv.FileStore, oc <-chan *packclient.Object, sentCommits, commonCommits [][]byte) {
	t.Helper()
	commonTables := map[string]struct{}{}
	commonRows := map[string]struct{}{}
	for _, sum := range commonCommits {
		commit, err := versioning.GetCommit(db, sum)
		require.NoError(t, err)
		commonTables[string(commit.Table)] = struct{}{}
		tbl, err := table.ReadTable(db, fs, commit.Table)
		require.NoError(t, err)
		rhr := tbl.NewRowHashReader(0, 0)
		for {
			_, sum, err := rhr.Read()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			commonRows[string(sum)] = struct{}{}
		}
	}
	commitMap := map[string]struct{}{}
	tableMap := map[string]struct{}{}
	rowMap := map[string]struct{}{}
	for obj := range oc {
		switch obj.Type {
		case encoding.ObjectCommit:
			sum := meow.Checksum(0, obj.Content)
			commitMap[string(sum[:])] = struct{}{}
		case encoding.ObjectTable:
			sum := meow.Checksum(0, obj.Content)
			tableMap[string(sum[:])] = struct{}{}
			_, ok := commonTables[string(sum[:])]
			assert.False(t, ok)
		case encoding.ObjectRow:
			sum := meow.Checksum(0, obj.Content)
			rowMap[string(sum[:])] = struct{}{}
			_, ok := commonRows[string(sum[:])]
			assert.False(t, ok)
		}
	}
	assert.Equal(t, len(sentCommits), len(commitMap))
	for _, sum := range sentCommits {
		if _, ok := commitMap[string(sum)]; !ok {
			t.Errorf("commit %x not found", sum)
			continue
		}
		commit, err := versioning.GetCommit(db, sum)
		require.NoError(t, err)
		_, ok1 := tableMap[string(commit.Table)]
		_, ok2 := commonTables[string(commit.Table)]
		if !ok1 && !ok2 {
			t.Errorf("table %x not found", commit.Table)
			continue
		}
		tbl, err := table.ReadTable(db, fs, commit.Table)
		require.NoError(t, err)
		rhr := tbl.NewRowHashReader(0, 0)
		for {
			_, sum, err := rhr.Read()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			_, ok1 := rowMap[string(sum)]
			_, ok2 := commonRows[string(sum)]
			if !ok1 && !ok2 {
				t.Errorf("row %x not found", sum)
				break
			}
		}
	}
}

func fetchObjects(t *testing.T, db kv.DB, fs kv.FileStore, advertised [][]byte, havesPerRoundTrip int) <-chan *packclient.Object {
	t.Helper()
	c, err := packclient.NewClient(testOrigin)
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	oc := make(chan *packclient.Object, 100)
	neg, err := packclient.NewNegotiator(db, fs, &wg, c, advertised, oc, havesPerRoundTrip)
	require.NoError(t, err)
	err = neg.Start()
	require.NoError(t, err)
	close(oc)
	return oc
}

func copyCommitsToNewStore(t *testing.T, dba, dbb kv.DB, fsa, fsb kv.FileStore, commits [][]byte) {
	t.Helper()
	for _, sum := range commits {
		c, err := versioning.GetCommit(dba, sum)
		require.NoError(t, err)
		_, err = versioning.SaveCommit(dbb, 0, c)
		require.NoError(t, err)
		tbl, err := table.ReadTable(dba, fsa, c.Table)
		require.NoError(t, err)
		builder := table.NewBuilder(dbb, fsb, tbl.Columns(), tbl.PrimaryKeyIndices(), 0, 0)
		r1 := tbl.NewRowReader()
		r2 := tbl.NewRowHashReader(0, 0)
		i := 0
		for {
			pk, sum, err := r2.Read()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			_, rc, err := r1.Read()
			require.NoError(t, err)
			require.NoError(t, builder.InsertRow(i, pk, sum, rc))
			i++
		}
		_, err = builder.SaveTable()
		require.NoError(t, err)
	}
}

func TestUploadPack(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	sum1, _ := createCommit(t, db, fs, nil)
	sum2, _ := createCommit(t, db, fs, [][]byte{sum1})
	sum3, _ := createCommit(t, db, fs, nil)
	sum4, _ := createCommit(t, db, fs, [][]byte{sum3})
	require.NoError(t, versioning.SaveHead(db, "main", sum2))
	require.NoError(t, versioning.SaveTag(db, "v1", sum4))
	register(http.MethodPost, "/upload-pack/", NewUploadPackHandler(db, fs))

	dbc := kv.NewMockStore(false)
	fsc := kv.NewMockStore(false)
	oc := fetchObjects(t, dbc, fsc, [][]byte{sum2}, 0)
	assertSentMissingCommits(t, db, fs, oc, [][]byte{sum1, sum2}, nil)

	copyCommitsToNewStore(t, db, dbc, fs, fsc, [][]byte{sum1})
	require.NoError(t, versioning.SaveHead(dbc, "main", sum1))
	oc = fetchObjects(t, dbc, fsc, [][]byte{sum2}, 0)
	assertSentMissingCommits(t, db, fs, oc, [][]byte{sum2}, [][]byte{sum1})

	copyCommitsToNewStore(t, db, dbc, fs, fsc, [][]byte{sum3})
	require.NoError(t, versioning.SaveTag(dbc, "v0", sum3))
	oc = fetchObjects(t, dbc, fsc, [][]byte{sum2, sum4}, 1)
	assertSentMissingCommits(t, db, fs, oc, [][]byte{sum2, sum4}, [][]byte{sum1, sum3})
}
