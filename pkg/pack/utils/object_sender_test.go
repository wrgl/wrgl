// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package packutils_test

import (
	"bytes"
	"encoding/csv"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	packtest "github.com/wrgl/core/pkg/pack/test"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/testutils"
)

func createRandomCommit(t *testing.T, db objects.Store, parents [][]byte) ([]byte, *objects.Commit) {
	rows := testutils.BuildRawCSV(5, 700)
	buf := bytes.NewBuffer(nil)
	w := csv.NewWriter(buf)
	require.NoError(t, w.WriteAll(rows))
	sum, err := ingest.IngestTable(db, io.NopCloser(bytes.NewReader(buf.Bytes())), rows[0][:1], 0, 1, io.Discard)
	require.NoError(t, err)
	com := &objects.Commit{
		Table:   sum,
		Parents: parents,
	}
	buf.Reset()
	_, err = com.WriteTo(buf)
	require.NoError(t, err)
	sum, err = objects.SaveCommit(db, buf.Bytes())
	require.NoError(t, err)
	return sum, com
}

func TestObjectSender(t *testing.T) {
	db1 := objmock.NewStore()
	db2 := objmock.NewStore()

	sum1, _ := createRandomCommit(t, db1, nil)
	packtest.CopyCommitsToNewStore(t, db1, db2, [][]byte{sum1})
	sum2, c2 := createRandomCommit(t, db1, [][]byte{sum1})
	sum3, c3 := createRandomCommit(t, db1, [][]byte{sum2})

	s, err := packutils.NewObjectSender(db1, []*objects.Commit{c2, c3}, [][]byte{sum1}, uint64(10*1024))
	require.NoError(t, err)
	r := packutils.NewObjectReceiver(db2, [][]byte{sum3}, []uint32{0})

	buf := bytes.NewBuffer(nil)
	require.NoError(t, s.WriteObjects(buf))

	pr, err := encoding.NewPackfileReader(io.NopCloser(buf))
	require.NoError(t, err)
	done, err := r.Receive(pr)
	require.NoError(t, err)
	assert.False(t, done)
}
