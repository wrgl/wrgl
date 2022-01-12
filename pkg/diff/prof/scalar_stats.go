// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package diffprof

import (
	"github.com/wrgl/wrgl/pkg/objects"
)

type statDiffFactory func(newTblProf, oldTblProf *objects.TableProfile, newColProf, oldColProf *objects.ColumnProfile) interface{}

type Uint16Stat struct {
	Name      string `json:"name"`
	ShortName string `json:"shortName"`
	Old       uint16 `json:"old"`
	New       uint16 `json:"new"`
}

func uint16StatFactory(name, sname string, getField func(col *objects.ColumnProfile) uint16) statDiffFactory {
	return func(newTblProf, oldTblProf *objects.TableProfile, newColProf, oldColProf *objects.ColumnProfile) interface{} {
		s := &Uint16Stat{
			Name:      name,
			ShortName: sname,
		}
		if oldColProf != nil {
			s.Old = getField(oldColProf)
		}
		if newColProf != nil {
			s.New = getField(newColProf)
		}
		if s.Old == 0 && s.New == 0 {
			return nil
		}
		return s
	}
}

func (s *Uint16Stat) Unchanged() bool {
	return s.Old == s.New
}

type Uint32Stat struct {
	Name      string `json:"name"`
	ShortName string `json:"shortName"`
	Old       uint32 `json:"old"`
	New       uint32 `json:"new"`
}

func uint32StatFactory(name, sname string, keep bool, getField func(col *objects.ColumnProfile) uint32) statDiffFactory {
	return func(newTblProf, oldTblProf *objects.TableProfile, newColProf, oldColProf *objects.ColumnProfile) interface{} {
		if oldColProf == nil && newColProf == nil {
			return nil
		}
		s := &Uint32Stat{
			Name:      name,
			ShortName: sname,
		}
		if oldColProf != nil {
			s.Old = getField(oldColProf)
		}
		if newColProf != nil {
			s.New = getField(newColProf)
		}
		if s.Old == 0 && s.New == 0 && !keep {
			return nil
		}
		return s
	}
}

func (s *Uint32Stat) Unchanged() bool {
	return s.Old == s.New
}

type Float64Stat struct {
	Name      string   `json:"name"`
	ShortName string   `json:"shortName"`
	Old       *float64 `json:"old"`
	New       *float64 `json:"new"`
}

func float64StatFactory(name, sname string, getField func(col *objects.ColumnProfile) *float64) statDiffFactory {
	return func(newTblProf, oldTblProf *objects.TableProfile, newColProf, oldColProf *objects.ColumnProfile) interface{} {
		s := &Float64Stat{
			Name:      name,
			ShortName: sname,
		}
		if oldColProf != nil {
			s.Old = getField(oldColProf)
		}
		if newColProf != nil {
			s.New = getField(newColProf)
		}
		if s.Old == nil && s.New == nil {
			return nil
		}
		return s
	}
}

func (s *Float64Stat) Unchanged() bool {
	return (s.Old == nil && s.New == nil) || *s.Old == *s.New
}
