// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package packutils_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	packtest "github.com/wrgl/core/pkg/pack/test"
	packutils "github.com/wrgl/core/pkg/pack/utils"
)

func TestObjectSender(t *testing.T) {
	db1 := objmock.NewStore()
	db2 := objmock.NewStore()

	sum1, _ := packtest.CreateRandomCommit(t, db1, 5, 700, nil)
	packtest.CopyCommitsToNewStore(t, db1, db2, [][]byte{sum1})
	sum2, c2 := packtest.CreateRandomCommit(t, db1, 5, 700, [][]byte{sum1})
	sum3, c3 := packtest.CreateRandomCommit(t, db1, 5, 700, [][]byte{sum2})

	s, err := packutils.NewObjectSender(db1, []*objects.Commit{c2, c3}, [][]byte{sum1}, uint64(10*1024))
	require.NoError(t, err)
	r := packutils.NewObjectReceiver(db2, [][]byte{sum3})

	buf := bytes.NewBuffer(nil)
	done := false
	for i := 0; ; i++ {
		buf.Reset()
		sendDone, err := s.WriteObjects(buf)
		require.NoError(t, err)

		pr, err := encoding.NewPackfileReader(io.NopCloser(buf))
		require.NoError(t, err)
		done, err = r.Receive(pr)
		require.NoError(t, err)
		assert.Equal(t, sendDone, done)
		if done {
			assert.Greater(t, i, 0)
			t.Logf("i:%d", i)
			break
		}
	}

	packtest.AssertCommitsPersisted(t, db2, [][]byte{sum2, sum3})
}
