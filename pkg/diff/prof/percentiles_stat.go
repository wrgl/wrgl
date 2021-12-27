// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diffprof

import (
	"github.com/wrgl/wrgl/pkg/objects"
)

type PercentileDiff struct {
	Old float64 `json:"old"`
	New float64 `json:"new"`
}

func comparePercentiles(newP, oldP []float64) []*PercentileDiff {
	result := []*PercentileDiff{}
	for _, f := range newP {
		result = append(result, &PercentileDiff{
			New: f,
		})
	}
	if len(newP) == 0 {
		for _, f := range oldP {
			result = append(result, &PercentileDiff{
				Old: f,
			})
		}
	} else if len(oldP) == len(newP) {
		for i, f := range oldP {
			result[i].Old = f
		}
	}
	return result
}

type PercentilesStat struct {
	Name        string            `json:"name"`
	ShortName   string            `json:"shortName"`
	NewAddition bool              `json:"newAddition,omitempty"`
	Removed     bool              `json:"removed,omitempty"`
	Values      []*PercentileDiff `json:"values"`
}

func percentilesStatFactory(name, sname string, getField func(col *objects.ColumnProfile) []float64) statDiffFactory {
	return func(newTblProf, oldTblProf *objects.TableProfile, newColProf, oldColProf *objects.ColumnProfile) interface{} {
		sd := &PercentilesStat{
			Name:      name,
			ShortName: sname,
		}
		var ov, nv []float64
		if oldColProf != nil {
			ov = getField(oldColProf)
		}
		if newColProf != nil {
			nv = getField(newColProf)
		}
		if nv == nil {
			if ov == nil {
				return nil
			}
			sd.Removed = true
		} else if ov == nil {
			sd.NewAddition = true
		}
		sd.Values = comparePercentiles(nv, ov)
		return sd
	}
}
