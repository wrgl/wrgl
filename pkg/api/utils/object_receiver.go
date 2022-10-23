// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiutils

import (
	"bytes"
	"fmt"
	"io"

	"github.com/go-logr/logr"
	"github.com/klauspost/compress/s2"
	"github.com/schollz/progressbar/v3"
	"github.com/wrgl/wrgl/pkg/encoding/packfile"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/objects"
)

type ObjectReceiver struct {
	db              objects.Store
	expectedCommits map[string]struct{}
	ReceivedCommits [][]byte
	debugLogger     *logr.Logger
	saveObjHook     func(objType int, sum []byte)
	buf             []byte
}

type ObjectReceiveOption func(r *ObjectReceiver)

func WithReceiverDebugLogger(out *logr.Logger) ObjectReceiveOption {
	return func(r *ObjectReceiver) {
		r.debugLogger = out
	}
}

// WithReceiverSaveObjectHook registers a hook that run right after an object is persisted successfully
func WithReceiverSaveObjectHook(hook func(objType int, sum []byte)) ObjectReceiveOption {
	return func(r *ObjectReceiver) {
		r.saveObjHook = hook
	}
}

func NewObjectReceiver(db objects.Store, expectedCommits [][]byte, opts ...ObjectReceiveOption) *ObjectReceiver {
	r := &ObjectReceiver{
		db:              db,
		expectedCommits: map[string]struct{}{},
	}
	for _, opt := range opts {
		opt(r)
	}
	for _, com := range expectedCommits {
		r.expectedCommits[string(com)] = struct{}{}
	}
	return r
}

func (r *ObjectReceiver) saveBlock(b []byte) (sum []byte, err error) {
	r.buf, err = s2.Decode(r.buf, b)
	if err != nil {
		return
	}
	if err = objects.ValidateBlockBytes(r.buf); err != nil {
		return
	}
	sum, err = objects.SaveCompressedBlock(r.db, r.buf, b)
	if r.saveObjHook != nil {
		r.saveObjHook(packfile.ObjectBlock, sum)
	}
	return
}

func (r *ObjectReceiver) saveTable(b []byte) (sum []byte, err error) {
	_, tbl, err := objects.ReadTableFrom(bytes.NewReader(b))
	if err != nil {
		return
	}
	sum, err = objects.SaveTable(r.db, b)
	if err != nil {
		return
	}
	if err = ingest.IndexTable(r.db, sum, tbl, r.debugLogger); err != nil {
		return
	}
	if err = ingest.ProfileTable(r.db, sum, tbl); err != nil {
		return
	}
	if r.saveObjHook != nil {
		r.saveObjHook(packfile.ObjectTable, sum)
	}
	return
}

func (r *ObjectReceiver) saveCommit(b []byte) (sum []byte, err error) {
	_, com, err := objects.ReadCommitFrom(bytes.NewReader(b))
	if err != nil {
		return
	}
	for _, parent := range com.Parents {
		if !objects.CommitExist(r.db, parent) {
			return nil, fmt.Errorf("parent commit %x does not exist", parent)
		}
	}
	sum, err = objects.SaveCommit(r.db, b)
	if err != nil {
		return
	}
	delete(r.expectedCommits, string(sum))
	r.ReceivedCommits = append(r.ReceivedCommits, sum)
	if r.saveObjHook != nil {
		r.saveObjHook(packfile.ObjectCommit, sum)
	}
	return
}

func (r *ObjectReceiver) Receive(pr *packfile.PackfileReader, pbar *progressbar.ProgressBar) (done bool, err error) {
	for {
		ot, b, err := pr.ReadObject()
		if err != nil && err != io.EOF {
			return false, fmt.Errorf("pr.ReadObject error: %v", err)
		}
		var sum []byte
		switch ot {
		case packfile.ObjectBlock:
			sum, err = r.saveBlock(b)
			if err != nil {
				return false, fmt.Errorf("r.saveBlock error: %v", err)
			}
		case packfile.ObjectTable:
			sum, err = r.saveTable(b)
			if err != nil {
				return false, fmt.Errorf("pr.ReadObject error: %v", err)
			}
		case packfile.ObjectCommit:
			sum, err = r.saveCommit(b)
			if err != nil {
				return false, fmt.Errorf("r.saveTable error: %v", err)
			}
		default:
			if ot != 0 || len(b) != 0 {
				return false, fmt.Errorf("unrecognized object type %d", ot)
			}
		}
		if ot != 0 {
			if err = pr.Info.AddObject(ot, sum); err != nil {
				return false, fmt.Errorf("pr.Info.AddObject error: %v", err)
			}
		}
		if ot != 0 && pbar != nil {
			if err = pbar.Add(1); err != nil {
				return false, fmt.Errorf("pbar.Add error: %v", err)
			}
		}
		if err == io.EOF {
			break
		}
	}
	return len(r.expectedCommits) == 0, nil
}
