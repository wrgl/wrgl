package main

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/testutils"
	"github.com/wrgl/core/pkg/versioning"
)

func TestHandleInfoRefs(t *testing.T) {
	db := kv.NewMockStore(false)
	fs := kv.NewMockStore(false)
	s := NewServer(db, fs)

	sum1 := testutils.SecureRandomBytes(16)
	head := "my-branch"
	err := versioning.SaveHead(db, head, sum1)
	require.NoError(t, err)
	sum2 := testutils.SecureRandomBytes(16)
	tag := "my-tag"
	err = versioning.SaveTag(db, tag, sum2)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/info/refs/", nil)
	err = s.HandleInfoRefs(rec, req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	parser := encoding.NewParser(rec.Body)
	str, err := encoding.ReadPktLine(parser)
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%s refs/heads/%s", hex.EncodeToString(sum1), head), str)
	str, err = encoding.ReadPktLine(parser)
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%s refs/tags/%s", hex.EncodeToString(sum2), tag), str)
	str, err = encoding.ReadPktLine(parser)
	require.NoError(t, err)
	assert.Equal(t, "", str)
}
