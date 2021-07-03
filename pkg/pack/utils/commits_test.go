// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package packutils_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/factory"
	"github.com/wrgl/core/pkg/objects"
	objmock "github.com/wrgl/core/pkg/objects/mock"
	packtest "github.com/wrgl/core/pkg/pack/test"
	packutils "github.com/wrgl/core/pkg/pack/utils"
)

func assertWroteRelevantCommitsToPackfile(t *testing.T, db objects.Store, commits, commonCommits [][]byte) {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	objs := []*objects.Commit{}
	for _, sum := range commits {
		obj, err := objects.GetCommit(db, sum)
		require.NoError(t, err)
		objs = append(objs, obj)
	}
	require.NoError(t, packutils.WriteCommitsToPackfile(db, objs, commonCommits, buf))
	r, err := encoding.NewPackfileReader(io.NopCloser(buf))
	require.NoError(t, err)
	oc := make(chan *packutils.Object, 1000)
	for {
		ot, ob, err := r.ReadObject()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		oc <- &packutils.Object{ot, ob}
	}
	close(oc)
	packtest.AssertSentMissingCommits(t, db, oc, commits, commonCommits)
}

func TestWriteCommitsToPackfile(t *testing.T) {
	db := objmock.NewStore()
	sum1, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, nil, nil)
	sum2, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"4,e,r",
		"5,d,f",
		"6,c,v",
	}, nil, nil)
	sum3, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,e",
		"2,a,s",
		"3,z,x",
	}, nil, [][]byte{sum1})
	sum4, _ := factory.Commit(t, db, []string{
		"a,b,c",
		"4,e,r",
		"5,d,f",
		"6,c,v",
	}, nil, nil)

	assertWroteRelevantCommitsToPackfile(t, db, [][]byte{sum2}, [][]byte{sum1})
	assertWroteRelevantCommitsToPackfile(t, db, [][]byte{sum3}, [][]byte{sum1})
	assertWroteRelevantCommitsToPackfile(t, db, [][]byte{sum4}, [][]byte{sum2})
}
