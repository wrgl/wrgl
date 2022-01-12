// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiclient "github.com/wrgl/wrgl/pkg/api/client"
	"github.com/wrgl/wrgl/pkg/api/payload"
	diffprof "github.com/wrgl/wrgl/pkg/diff/prof"
	"github.com/wrgl/wrgl/pkg/factory"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func uint32Ptr(u uint32) *uint32 {
	return &u
}

func (s *testSuite) TestDiffHandler(t *testing.T) {
	repo, cli, m, cleanup := s.s.NewClient(t, true, "", nil)
	defer cleanup()
	db := s.s.GetDB(repo)

	sum1, com1 := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"4,z,x",
	}, []uint32{0}, nil)
	sum2, com2 := factory.Commit(t, db, []string{
		"a,b,d",
		"1,q,e",
		"2,a,d",
		"5,z,c",
	}, []uint32{0}, nil)
	sum3, com3 := factory.Commit(t, db, []string{
		"a,b,c",
		"1,q,w",
		"2,a,s",
		"4,z,x",
	}, []uint32{0}, nil)

	_, err := cli.Diff(testutils.SecureRandomBytes(16), sum2)
	assertHTTPError(t, err, http.StatusNotFound, "Not Found")

	_, err = cli.Diff(sum1, testutils.SecureRandomBytes(16))
	assertHTTPError(t, err, http.StatusNotFound, "Not Found")

	dr, err := cli.Diff(sum1, sum2)
	require.NoError(t, err)
	assert.Equal(t, &payload.DiffResponse{
		TableSum:    payload.BytesToHex(com1.Table),
		OldTableSum: payload.BytesToHex(com2.Table),
		Columns:     []string{"a", "b", "c"},
		OldColumns:  []string{"a", "b", "d"},
		PK:          []uint32{0},
		OldPK:       []uint32{0},
		RowDiff: []*payload.RowDiff{
			{
				Offset1: uint32Ptr(0),
				Offset2: uint32Ptr(0),
			},
			{
				Offset1: uint32Ptr(1),
				Offset2: uint32Ptr(1),
			},
			{
				Offset1: uint32Ptr(2),
			},
			{
				Offset2: uint32Ptr(2),
			},
		},
		DataProfile: &diffprof.TableProfileDiff{
			NewRowsCount: 3,
			OldRowsCount: 3,
			Columns: []*diffprof.ColumnProfileDiff{
				{
					Name: "a",
					Stats: []interface{}{
						map[string]interface{}{
							"name":      "NA count",
							"new":       float64(0),
							"old":       float64(0),
							"shortName": "naCount",
						},
						map[string]interface{}{
							"name":      "Min",
							"new":       float64(1),
							"old":       float64(1),
							"shortName": "min",
						},
						map[string]interface{}{
							"name":      "Max",
							"new":       float64(4),
							"old":       float64(5),
							"shortName": "max",
						},
						map[string]interface{}{
							"name":      "Mean",
							"new":       float64(2.33),
							"old":       float64(2.67),
							"shortName": "mean",
						},
						map[string]interface{}{
							"name":      "Median",
							"new":       float64(2),
							"old":       float64(2),
							"shortName": "median",
						},
						map[string]interface{}{
							"name":      "Std. deviation",
							"new":       float64(1.25),
							"old":       float64(1.7),
							"shortName": "stdDeviation",
						},
						map[string]interface{}{
							"name":      "Min length",
							"new":       float64(1),
							"old":       float64(1),
							"shortName": "minStrLen",
						},
						map[string]interface{}{
							"name":      "Max length",
							"new":       float64(1),
							"old":       float64(1),
							"shortName": "maxStrLen",
						},
						map[string]interface{}{
							"name":      "Avg. length",
							"new":       float64(1),
							"old":       float64(1),
							"shortName": "avgStrLen",
						},
					},
				},

				{
					Name:        "b",
					NewAddition: false,
					Removed:     false,
					Stats: []interface{}{
						map[string]interface{}{
							"name":      "NA count",
							"new":       float64(0),
							"old":       float64(0),
							"shortName": "naCount",
						},
						map[string]interface{}{
							"name":      "Min length",
							"new":       float64(1),
							"old":       float64(1),
							"shortName": "minStrLen",
						},
						map[string]interface{}{
							"name":      "Max length",
							"new":       float64(1),
							"old":       float64(1),
							"shortName": "maxStrLen",
						},
						map[string]interface{}{
							"name":      "Avg. length",
							"new":       float64(1),
							"old":       float64(1),
							"shortName": "avgStrLen",
						},
					},
				},
				{
					Name:        "c",
					NewAddition: true,
					Removed:     false,
					Stats: []interface{}{
						map[string]interface{}{
							"name":      "NA count",
							"new":       float64(0),
							"old":       float64(0),
							"shortName": "naCount",
						},
						map[string]interface{}{
							"name":      "Min length",
							"new":       float64(1),
							"old":       float64(0),
							"shortName": "minStrLen",
						},
						map[string]interface{}{
							"name":      "Max length",
							"new":       float64(1),
							"old":       float64(0),
							"shortName": "maxStrLen",
						},
						map[string]interface{}{
							"name":      "Avg. length",
							"new":       float64(1),
							"old":       float64(0),
							"shortName": "avgStrLen",
						},
					},
				},
				{
					Name:        "d",
					NewAddition: false,
					Removed:     true,
					Stats: []interface{}{
						map[string]interface{}{
							"name":      "NA count",
							"new":       float64(0),
							"old":       float64(0),
							"shortName": "naCount",
						},
						map[string]interface{}{
							"name":      "Min length",
							"new":       float64(0),
							"old":       float64(1),
							"shortName": "minStrLen",
						},
						map[string]interface{}{
							"name":      "Max length",
							"new":       float64(0),
							"old":       float64(1),
							"shortName": "maxStrLen",
						},
						map[string]interface{}{
							"name":      "Avg. length",
							"new":       float64(0),
							"old":       float64(1),
							"shortName": "avgStrLen",
						},
					},
				},
			},
		},
	}, dr)

	dr, err = cli.Diff(sum1, sum3)
	require.NoError(t, err)
	assert.Equal(t, &payload.DiffResponse{
		TableSum:    payload.BytesToHex(com1.Table),
		OldTableSum: payload.BytesToHex(com3.Table),
		Columns:     []string{"a", "b", "c"},
		OldColumns:  []string{"a", "b", "c"},
		PK:          []uint32{0},
		OldPK:       []uint32{0},
	}, dr)

	// pass custom headers
	req := m.Capture(t, func(header http.Header) {
		header.Set("Asdf", "1234")
		dr, err = cli.Diff(sum1, sum2, apiclient.WithRequestHeader(header))
		require.NoError(t, err)
		assert.NotEmpty(t, dr)
	})
	assert.Equal(t, "1234", req.Header.Get("Asdf"))
}
