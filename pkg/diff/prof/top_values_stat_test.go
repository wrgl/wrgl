// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package diffprof

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
)

func TestTopValuesStat(t *testing.T) {
	f := topValuesStatFactory("Top values", "topValues", func(col *objects.ColumnProfile) objects.ValueCounts { return col.TopValues })
	s := f(
		&objects.TableProfile{RowsCount: 100}, &objects.TableProfile{RowsCount: 100},
		&objects.ColumnProfile{TopValues: objects.ValueCounts{
			{Value: "A", Count: 10},
			{Value: "B", Count: 8},
			{Value: "C", Count: 5},
		}},
		&objects.ColumnProfile{TopValues: objects.ValueCounts{
			{Value: "A", Count: 10},
			{Value: "B", Count: 6},
			{Value: "D", Count: 4},
		}},
	)
	assert.Equal(t, &TopValuesStat{
		Name:      "Top values",
		ShortName: "topValues",
		Values: []ValueCountDiff{
			{
				Value:    "A",
				OldCount: 10,
				NewCount: 10,
				OldPct:   10,
				NewPct:   10,
			},
			{
				Value:    "B",
				OldCount: 6,
				NewCount: 8,
				OldPct:   6,
				NewPct:   8,
			},
			{
				Value:    "D",
				OldCount: 4,
				OldPct:   4,
			},
			{
				Value:    "C",
				NewCount: 5,
				NewPct:   5,
			},
		},
	}, s)
	b, err := json.Marshal(s)
	require.NoError(t, err)
	assert.Equal(t,
		"{\"name\":\"Top values\",\"shortName\":\"topValues\",\"values\":[{\"value\":\"A\",\"oldCount\":10,\"newCount\":10,\"oldPct\":10,\"newPct\":10},{\"value\":\"B\",\"oldCount\":6,\"newCount\":8,\"oldPct\":6,\"newPct\":8},{\"value\":\"D\",\"oldCount\":4,\"newCount\":0,\"oldPct\":4,\"newPct\":0},{\"value\":\"C\",\"oldCount\":0,\"newCount\":5,\"oldPct\":0,\"newPct\":5}]}",
		string(b),
	)

	assert.Equal(t,
		&TopValuesStat{
			Name:      "Top values",
			ShortName: "topValues",
			Removed:   true,
			Values: []ValueCountDiff{
				{
					Value:    "A",
					OldCount: 10,
					OldPct:   10,
				},
				{
					Value:    "B",
					OldCount: 6,
					OldPct:   6,
				},
				{
					Value:    "D",
					OldCount: 4,
					OldPct:   4,
				},
			},
		},
		f(
			&objects.TableProfile{RowsCount: 100}, &objects.TableProfile{RowsCount: 100},
			nil,
			&objects.ColumnProfile{TopValues: objects.ValueCounts{
				{Value: "A", Count: 10},
				{Value: "B", Count: 6},
				{Value: "D", Count: 4},
			}},
		),
	)

	assert.Equal(t,
		&TopValuesStat{
			Name:      "Top values",
			ShortName: "topValues",
			Removed:   true,
			Values: []ValueCountDiff{
				{
					Value:    "A",
					OldCount: 10,
					OldPct:   10,
				},
				{
					Value:    "B",
					OldCount: 6,
					OldPct:   6,
				},
				{
					Value:    "D",
					OldCount: 4,
					OldPct:   4,
				},
			},
		},
		f(
			&objects.TableProfile{RowsCount: 100}, &objects.TableProfile{RowsCount: 100},
			&objects.ColumnProfile{},
			&objects.ColumnProfile{TopValues: objects.ValueCounts{
				{Value: "A", Count: 10},
				{Value: "B", Count: 6},
				{Value: "D", Count: 4},
			}},
		),
	)

	assert.Equal(t,
		&TopValuesStat{
			Name:        "Top values",
			ShortName:   "topValues",
			NewAddition: true,
			Values: []ValueCountDiff{
				{
					Value:    "A",
					NewCount: 10,
					NewPct:   10,
				},
				{
					Value:    "B",
					NewCount: 8,
					NewPct:   8,
				},
				{
					Value:    "C",
					NewCount: 5,
					NewPct:   5,
				},
			},
		},
		f(
			&objects.TableProfile{RowsCount: 100}, &objects.TableProfile{RowsCount: 100},
			&objects.ColumnProfile{TopValues: objects.ValueCounts{
				{Value: "A", Count: 10},
				{Value: "B", Count: 8},
				{Value: "C", Count: 5},
			}},
			nil,
		),
	)

	assert.Equal(t,
		&TopValuesStat{
			Name:        "Top values",
			ShortName:   "topValues",
			NewAddition: true,
			Values: []ValueCountDiff{
				{
					Value:    "A",
					NewCount: 10,
					NewPct:   10,
				},
				{
					Value:    "B",
					NewCount: 8,
					NewPct:   8,
				},
				{
					Value:    "C",
					NewCount: 5,
					NewPct:   5,
				},
			},
		},
		f(
			&objects.TableProfile{RowsCount: 100}, &objects.TableProfile{RowsCount: 100},
			&objects.ColumnProfile{TopValues: objects.ValueCounts{
				{Value: "A", Count: 10},
				{Value: "B", Count: 8},
				{Value: "C", Count: 5},
			}},
			&objects.ColumnProfile{},
		),
	)

	assert.Nil(t, f(&objects.TableProfile{RowsCount: 100}, &objects.TableProfile{RowsCount: 100}, nil, nil))
	assert.Nil(t, f(
		&objects.TableProfile{RowsCount: 100}, &objects.TableProfile{RowsCount: 100},
		&objects.ColumnProfile{}, &objects.ColumnProfile{},
	))
}
