// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package stats

import (
	"encoding/json"

	"github.com/wrgl/wrgl/pkg/objects"
)

type ColumnSummaryDiff struct {
	Name        string           `json:"name"`
	NewAddition bool             `json:"newAddition"`
	Removed     bool             `json:"removed"`
	Stats       []json.Marshaler `json:"stats"`
}

var (
	statDiffFactories []statDiffFactory
)

func init() {
	statDiffFactories = []statDiffFactory{
		uint32StatFactory("NA count", "naCount", func(col *objects.ColumnSummary) uint32 { return col.NACount }),
		float64StatFactory("Min", "min", func(col *objects.ColumnSummary) *float64 { return col.Min }),
		float64StatFactory("Max", "max", func(col *objects.ColumnSummary) *float64 { return col.Max }),
		float64StatFactory("Mean", "mean", func(col *objects.ColumnSummary) *float64 { return col.Mean }),
		float64StatFactory("Median", "median", func(col *objects.ColumnSummary) *float64 { return col.Median }),
		float64StatFactory("Standard deviation", "stdDeviation", func(col *objects.ColumnSummary) *float64 { return col.StdDeviation }),
		uint16StatFactory("Min string length", "minStrLen", func(col *objects.ColumnSummary) uint16 { return col.MinStrLen }),
		uint16StatFactory("Max string length", "maxStrLen", func(col *objects.ColumnSummary) uint16 { return col.MaxStrLen }),
		uint16StatFactory("Avg string length", "avgStrLen", func(col *objects.ColumnSummary) uint16 { return col.AvgStrLen }),
		topValuesStatFactory("Top values", "topValues", func(col *objects.ColumnSummary) objects.ValueCounts { return col.TopValues }),
		percentilesStatFactory("Percentiles", "percentiles", func(col *objects.ColumnSummary) []float64 { return col.Percentiles }),
	}
}

func (c *ColumnSummaryDiff) CollectStats(newTblSum, oldTblSum *objects.TableSummary, newColSum, oldColSum *objects.ColumnSummary) {
	for _, f := range statDiffFactories {
		if stat := f(newTblSum, oldTblSum, newColSum, oldColSum); stat != nil {
			c.Stats = append(c.Stats, stat)
		}
	}
}
