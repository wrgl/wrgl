// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objects_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	objhelpers "github.com/wrgl/wrgl/pkg/objects/helpers"
)

func TestWriteTransaction(t *testing.T) {
	tx := &objects.Transaction{
		Status: objects.TSInProgress,
		Begin:  time.Now().Add(-time.Hour),
	}
	buf := bytes.NewBufferString("")
	n, err := tx.WriteTo(buf)
	require.NoError(t, err)
	assert.Len(t, buf.Bytes(), int(n))
	n, tx2, err := objects.ReadTransactionFrom(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Len(t, buf.Bytes(), int(n))
	objhelpers.AssertTransactionEqual(t, tx, tx2)

	tx.Status = objects.TSCommitted
	tx.End = time.Now()
	buf.Reset()
	n, err = tx.WriteTo(buf)
	assert.Len(t, buf.Bytes(), int(n))
	n, tx2, err = objects.ReadTransactionFrom(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Len(t, buf.Bytes(), int(n))
	objhelpers.AssertTransactionEqual(t, tx, tx2)
}
