// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diffprof

import (
	"encoding/json"

	"github.com/wrgl/wrgl/pkg/objects"
)

type statDiffFactory func(newTblProf, oldTblProf *objects.TableProfile, newColProf, oldColProf *objects.ColumnProfile) json.Marshaler

type uint16Stat struct {
	Name      string `json:"name"`
	ShortName string `json:"shortName"`
	Old       uint16 `json:"old"`
	New       uint16 `json:"new"`
}

func (s *uint16Stat) MarshalJSON() ([]byte, error) {
	return json.Marshal(s)
}

func uint16StatFactory(name, sname string, getField func(col *objects.ColumnProfile) uint16) statDiffFactory {
	return func(newTblProf, oldTblProf *objects.TableProfile, newColProf, oldColProf *objects.ColumnProfile) json.Marshaler {
		s := &uint16Stat{
			Name:      name,
			ShortName: sname,
		}
		if oldColProf != nil {
			s.Old = getField(oldColProf)
		}
		if newColProf != nil {
			s.New = getField(newColProf)
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

func uint32StatFactory(name, sname string, getField func(col *objects.ColumnProfile) uint32) statDiffFactory {
	return func(newTblProf, oldTblProf *objects.TableProfile, newColProf, oldColProf *objects.ColumnProfile) json.Marshaler {
		s := &uint32Stat{
			Name:      name,
			ShortName: sname,
		}
		if oldColProf != nil {
			s.Old = getField(oldColProf)
		}
		if newColProf != nil {
			s.New = getField(newColProf)
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

func float64StatFactory(name, sname string, getField func(col *objects.ColumnProfile) *float64) statDiffFactory {
	return func(newTblProf, oldTblProf *objects.TableProfile, newColProf, oldColProf *objects.ColumnProfile) json.Marshaler {
		s := &float64Stat{
			Name:      name,
			ShortName: sname,
		}
		if oldColProf != nil {
			s.Old = getField(oldColProf)
		}
		if newColProf != nil {
			s.New = getField(newColProf)
		}
		return s
	}
}
