// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package stats

import (
	"encoding/json"
	"math"

	"github.com/wrgl/wrgl/pkg/objects"
)

type ValueCountDiff struct {
	Value    string `json:"value"`
	OldCount uint32 `json:"oldCount"`
	NewCount uint32 `json:"newCount"`
	OldPct   byte   `json:"oldPct"`
	NewPct   byte   `json:"newPct"`
}

func compareValueCounts(newRowsCount, oldRowsCount uint32, newVC, oldVC objects.ValueCounts) []ValueCountDiff {
	result := []ValueCountDiff{}
	newM := map[string]uint32{}
	for _, vc := range newVC {
		newM[vc.Value] = vc.Count
	}
	oldM := map[string]uint32{}
	for _, vc := range oldVC {
		oldM[vc.Value] = vc.Count
		vcd := ValueCountDiff{
			Value:    vc.Value,
			OldCount: vc.Count,
			OldPct:   byte(math.Round(float64(vc.Count) / float64(oldRowsCount) * 100)),
		}
		if c, ok := newM[vc.Value]; ok {
			vcd.NewCount = c
			vcd.NewPct = byte(math.Round(float64(c) / float64(newRowsCount) * 100))
			if vcd.NewCount == vcd.OldCount && vcd.NewPct == vcd.OldPct {
				continue
			}
		}
		result = append(result, vcd)
	}
	for _, vc := range newVC {
		if _, ok := oldM[vc.Value]; ok {
			continue
		}
		vcd := ValueCountDiff{
			Value:    vc.Value,
			NewCount: vc.Count,
			NewPct:   byte(math.Round(float64(vc.Count) / float64(newRowsCount) * 100)),
		}
		result = append(result, vcd)
	}
	return result
}

type TopValuesStat struct {
	Name        string           `json:"name"`
	ShortName   string           `json:"shortName"`
	NewAddition bool             `json:"newAddition"`
	Removed     bool             `json:"removed"`
	Values      []ValueCountDiff `json:"values"`
}

func (s *TopValuesStat) MarshalJSON() ([]byte, error) {
	return json.Marshal(s)
}

func topValuesStatFactory(name, sname string, getField func(col *objects.ColumnSummary) objects.ValueCounts) statDiffFactory {
	return func(newTblSum, oldTblSum *objects.TableSummary, newColSum, oldColSum *objects.ColumnSummary) json.Marshaler {
		sd := &TopValuesStat{
			Name:      name,
			ShortName: sname,
		}
		var ov, nv objects.ValueCounts
		if oldColSum != nil {
			ov = getField(oldColSum)
		}
		if newColSum != nil {
			nv = getField(newColSum)
		}
		if nv.IsEmpty() {
			if ov.IsEmpty() {
				return nil
			}
			sd.Removed = true
		} else if ov.IsEmpty() {
			sd.NewAddition = true
		}
		sd.Values = compareValueCounts(newTblSum.RowsCount, oldTblSum.RowsCount, nv, ov)
		return sd
	}
}
