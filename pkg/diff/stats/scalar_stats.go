// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package stats

import (
	"encoding/json"

	"github.com/wrgl/wrgl/pkg/objects"
)

type statDiffFactory func(newTblSum, oldTblSum *objects.TableSummary, newColSum, oldColSum *objects.ColumnSummary) json.Marshaler

type uint16Stat struct {
	Name      string `json:"name"`
	ShortName string `json:"shortName"`
	Old       uint16 `json:"old"`
	New       uint16 `json:"new"`
}

func (s *uint16Stat) MarshalJSON() ([]byte, error) {
	return json.Marshal(s)
}

func uint16StatFactory(name, sname string, getField func(col *objects.ColumnSummary) uint16) statDiffFactory {
	return func(newTblSum, oldTblSum *objects.TableSummary, newColSum, oldColSum *objects.ColumnSummary) json.Marshaler {
		s := &uint16Stat{
			Name:      name,
			ShortName: sname,
		}
		if oldColSum != nil {
			s.Old = getField(oldColSum)
		}
		if newColSum != nil {
			s.New = getField(newColSum)
		}
		return s
	}
}

type uint32Stat struct {
	Name      string `json:"name"`
	ShortName string `json:"shortName"`
	Old       uint32 `json:"old"`
	New       uint32 `json:"new"`
}

func (s *uint32Stat) MarshalJSON() ([]byte, error) {
	return json.Marshal(s)
}

func uint32StatFactory(name, sname string, getField func(col *objects.ColumnSummary) uint32) statDiffFactory {
	return func(newTblSum, oldTblSum *objects.TableSummary, newColSum, oldColSum *objects.ColumnSummary) json.Marshaler {
		s := &uint32Stat{
			Name:      name,
			ShortName: sname,
		}
		if oldColSum != nil {
			s.Old = getField(oldColSum)
		}
		if newColSum != nil {
			s.New = getField(newColSum)
		}
		return s
	}
}

type float64Stat struct {
	Name      string   `json:"name"`
	ShortName string   `json:"shortName"`
	Old       *float64 `json:"old"`
	New       *float64 `json:"new"`
}

func (s *float64Stat) MarshalJSON() ([]byte, error) {
	return json.Marshal(s)
}

func float64StatFactory(name, sname string, getField func(col *objects.ColumnSummary) *float64) statDiffFactory {
	return func(newTblSum, oldTblSum *objects.TableSummary, newColSum, oldColSum *objects.ColumnSummary) json.Marshaler {
		s := &float64Stat{
			Name:      name,
			ShortName: sname,
		}
		if oldColSum != nil {
			s.Old = getField(oldColSum)
		}
		if newColSum != nil {
			s.New = getField(newColSum)
		}
		return s
	}
}
