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
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	packtest "github.com/wrgl/core/pkg/pack/test"
	packutils "github.com/wrgl/core/pkg/pack/utils"
	"github.com/wrgl/core/pkg/versioning"
)

func assertWroteRelevantCommitsToPackfile(t *testing.T, db kv.DB, fs kv.FileStore, commits, commonCommits [][]byte) {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	objs := []*objects.Commit{}
	for _, sum := range commits {
		obj, err := versioning.GetCommit(db, sum)
		require.NoError(t, err)
		objs = append(objs, obj)
	}
	require.NoError(t, packutils.WriteCommitsToPackfile(db, fs, objs, commonCommits, buf))
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
	packtest.AssertSentMissingCommits(t, db, fs, oc, commits, commonCommits)
}

func TestWriteCommitsToPackfile(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	sum1, _ := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"3,z,x",
	}, nil, nil)
	sum2, _ := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"4,e,r",
		"5,d,f",
		"6,c,v",
	}, nil, nil)
	sum3, _ := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"1,q,e",
		"2,a,s",
		"3,z,x",
	}, nil, [][]byte{sum1})
	sum4, _ := factory.Commit(t, db, fs, []string{
		"a,b,c",
		"4,e,r",
		"5,d,f",
		"6,c,v",
	}, nil, nil)

	assertWroteRelevantCommitsToPackfile(t, db, fs, [][]byte{sum2}, [][]byte{sum1})
	assertWroteRelevantCommitsToPackfile(t, db, fs, [][]byte{sum3}, [][]byte{sum1})
	assertWroteRelevantCommitsToPackfile(t, db, fs, [][]byte{sum4}, [][]byte{sum2})
}
