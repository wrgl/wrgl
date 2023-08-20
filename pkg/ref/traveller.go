// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ref

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/wrgl/wrgl/pkg/objects"
)

// Traveller travels a ref from the latest commit to the very first commit of that ref
type Traveller struct {
	name   string
	db     objects.Store
	reader ReflogReader
	com    *objects.Commit
	done   bool
	// Reflog is the closest reflog to the current commit, provided that all traversal is done via Parent()
	Reflog  *Reflog
	nextLog *Reflog
}

// NewTraveller creates a new BranchTraveller
func NewTraveller(db objects.Store, rs Store, refName string) (*Traveller, error) {
	reader, err := rs.LogReader(refName)
	if err != nil {
		return nil, fmt.Errorf("rs.LogReader err: %v", err)
	}
	nxt, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("reader.Read err: %v", err)
	}
	rt := &Traveller{
		name:    refName,
		reader:  reader,
		nextLog: nxt,
		db:      db,
	}
	if err := rt.readReflog(); err != nil {
		return nil, fmt.Errorf("rt.readReflog err: %v", err)
	}
	return rt, nil
}

func (rt *Traveller) readReflog() error {
	if rt.nextLog != nil {
		rt.Reflog = rt.nextLog
		rt.nextLog = nil
	}
	if rt.reader != nil {
		rl, err := rt.reader.Read()
		if err != nil && err != io.EOF {
			return err
		}
		rt.nextLog = rl
		if errors.Is(err, io.EOF) {
			rt.reader.Close()
			rt.reader = nil
		}
	}
	return nil
}

// Next traverses the ref and returns the next commit. If it returns
// nil then that means there's no more commit in this ref.
func (rt *Traveller) Next() (*objects.Commit, error) {
	var err error
	if rt.com == nil {
		if rt.done {
			return nil, nil
		}
		rt.com, err = objects.GetCommit(rt.db, rt.Reflog.NewOID)
	} else {
		if rt.Reflog.OldOID == nil && rt.Reflog.Action != "fetch" {
			rt.com = nil
			rt.done = true
		} else {
			switch len(rt.com.Parents) {
			case 0:
				rt.com = nil
				rt.done = true
			case 1:
				rt.com, err = objects.GetCommit(rt.db, rt.com.Parents[0])
			default:
				rt.com, err = objects.GetCommit(rt.db, rt.Reflog.OldOID)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if rt.nextLog != nil && rt.com != nil && bytes.Equal(rt.nextLog.NewOID, rt.com.Sum) {
		if err := rt.readReflog(); err != nil {
			return nil, err
		}
	}
	return rt.com, nil
}

func (rt *Traveller) Close() error {
	if rt.reader != nil {
		return rt.reader.Close()
	}
	return nil
}
