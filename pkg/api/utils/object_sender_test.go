// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiutils_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apitest "github.com/wrgl/wrgl/pkg/api/test"
	apiutils "github.com/wrgl/wrgl/pkg/api/utils"
	"github.com/wrgl/wrgl/pkg/encoding/packfile"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
)

func sendAll(t *testing.T, sender *apiutils.ObjectSender, receiver *apiutils.ObjectReceiver) {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	done := false
	for i := 0; ; i++ {
		buf.Reset()
		sendDone, err := sender.WriteObjects(buf, nil)
		require.NoError(t, err)

		pr, err := packfile.NewPackfileReader(io.NopCloser(buf))
		require.NoError(t, err)
		done, err = receiver.Receive(pr, nil)
		require.NoError(t, err)
		assert.Equal(t, sendDone, done)
		if done {
			assert.Greater(t, i, 0)
			t.Logf("i:%d", i)
			break
		}
	}
}

func TestObjectSender(t *testing.T) {
	db1 := objmock.NewStore()
	db2 := objmock.NewStore()

	sum1, _ := apitest.CreateRandomCommit(t, db1, 5, 700, nil)
	apitest.CopyCommitsToNewStore(t, db1, db2, [][]byte{sum1})
	sum2, c2 := apitest.CreateRandomCommit(t, db1, 5, 700, [][]byte{sum1})
	sum3, c3 := apitest.CreateRandomCommit(t, db1, 5, 700, [][]byte{sum2})

	tables := map[string]struct{}{
		string(c2.Table): {},
		string(c3.Table): {},
	}
	s, err := apiutils.NewObjectSender(db1, []*objects.Commit{c2, c3}, tables, [][]byte{sum1}, uint64(10*1024))
	require.NoError(t, err)
	r := apiutils.NewObjectReceiver(db2, [][]byte{sum3}, nil)
	sendAll(t, s, r)
	apitest.AssertCommitsPersisted(t, db2, [][]byte{sum2, sum3})
}

func TestSendCommitsWithIdenticalTable(t *testing.T) {
	db1 := objmock.NewStore()
	db2 := objmock.NewStore()

	sum1, c1 := apitest.CreateRandomCommit(t, db1, 5, 700, nil)
	sum2, c2 := apitest.CreateRandomCommit(t, db1, 5, 700, nil)
	sum3, c3 := apitest.CreateRandomCommitWithTable(t, db1, c1.Table, [][]byte{sum2})

	tables := map[string]struct{}{
		string(c1.Table): {},
		string(c2.Table): {},
	}
	s, err := apiutils.NewObjectSender(db1, []*objects.Commit{c1, c2, c3}, tables, nil, uint64(10*1024))
	require.NoError(t, err)
	r := apiutils.NewObjectReceiver(db2, [][]byte{sum1, sum3}, nil)
	sendAll(t, s, r)
	apitest.AssertCommitsPersisted(t, db2, [][]byte{sum1, sum2, sum3})
}
