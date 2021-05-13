package pack_test

import (
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/mmcloughlin/meow"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/pack"
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
		case encoding.ObjectRow:
			sum := meow.Checksum(0, obj.Content)
			rowMap[string(sum[:])] = struct{}{}
		}
	}
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

func TestUploadPack(t *testing.T) {
	httpmock.Activate()
	defer httpmock.Deactivate()
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	sum1, _ := createCommit(t, db, fs, nil)
	sum2, _ := createCommit(t, db, fs, [][]byte{sum1})
	require.NoError(t, versioning.SaveHead(db, "main", sum2))
	register(http.MethodPost, "/upload-pack/", pack.NewUploadPackHandler(db, fs))

	c, err := packclient.NewClient(testOrigin)
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	oc := make(chan *packclient.Object, 100)
	dbc := kv.NewMockStore(false)
	fsc := kv.NewMockStore(false)
	neg, err := packclient.NewNegotiator(dbc, fsc, &wg, c, [][]byte{sum2}, oc)
	require.NoError(t, err)
	err = neg.Start()
	require.NoError(t, err)
	close(oc)

	assertSentMissingCommits(t, db, fs, oc, [][]byte{sum1, sum2}, nil)
}
