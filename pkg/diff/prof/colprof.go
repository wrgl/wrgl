// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diffprof

import (
	"github.com/wrgl/wrgl/pkg/objects"
)

type ColumnProfileDiff struct {
	Name        string        `json:"name"`
	NewAddition bool          `json:"newAddition,omitempty"`
	Removed     bool          `json:"removed,omitempty"`
	Stats       []interface{} `json:"stats"`
}

var (
	statDiffFactories []statDiffFactory
)

func init() {
	statDiffFactories = []statDiffFactory{
		uint32StatFactory("NA count", "naCount", true, func(col *objects.ColumnProfile) uint32 { return col.NACount }),
		float64StatFactory("Min", "min", func(col *objects.ColumnProfile) *float64 { return col.Min }),
		float64StatFactory("Max", "max", func(col *objects.ColumnProfile) *float64 { return col.Max }),
		float64StatFactory("Mean", "mean", func(col *objects.ColumnProfile) *float64 { return col.Mean }),
		float64StatFactory("Median", "median", func(col *objects.ColumnProfile) *float64 { return col.Median }),
		float64StatFactory("Std. deviation", "stdDeviation", func(col *objects.ColumnProfile) *float64 { return col.StdDeviation }),
		uint16StatFactory("Min length", "minStrLen", func(col *objects.ColumnProfile) uint16 { return col.MinStrLen }),
		uint16StatFactory("Max length", "maxStrLen", func(col *objects.ColumnProfile) uint16 { return col.MaxStrLen }),
		uint16StatFactory("Avg. length", "avgStrLen", func(col *objects.ColumnProfile) uint16 { return col.AvgStrLen }),
		topValuesStatFactory("Top values", "topValues", func(col *objects.ColumnProfile) objects.ValueCounts { return col.TopValues }),
		percentilesStatFactory("Percentiles", "percentiles", func(col *objects.ColumnProfile) []float64 { return col.Percentiles }),
	}
}

func (c *ColumnProfileDiff) CollectStats(newTblSum, oldTblSum *objects.TableProfile, newColSum, oldColSum *objects.ColumnProfile) {
	for _, f := range statDiffFactories {
		if stat := f(newTblSum, oldTblSum, newColSum, oldColSum); stat != nil {
			c.Stats = append(c.Stats, stat)
		}
	}
}

func (c *ColumnProfileDiff) Unchanged() bool {
	if c.NewAddition || c.Removed {
		return false
	}
	for _, v := range c.Stats {
		if !v.(diffobj).Unchanged() {
			return false
		}
	}
	return true
}
