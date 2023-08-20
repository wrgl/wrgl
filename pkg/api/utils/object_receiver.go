// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiutils

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-logr/logr"
	"github.com/klauspost/compress/s2"
	"github.com/wrgl/wrgl/pkg/encoding/packfile"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/pbar"
)

type ObjectReceiver struct {
	db              objects.Store
	expectedCommits map[string]struct{}
	ReceivedCommits [][]byte
	logger          logr.Logger
	saveObjHook     func(objType int, sum []byte)
	buf             []byte
}

type ObjectReceiveOption func(r *ObjectReceiver)

// WithReceiverSaveObjectHook registers a hook that run right after an object is persisted successfully
func WithReceiverSaveObjectHook(hook func(objType int, sum []byte)) ObjectReceiveOption {
	return func(r *ObjectReceiver) {
		r.saveObjHook = hook
	}
}

func NewObjectReceiver(db objects.Store, expectedCommits [][]byte, logger logr.Logger, opts ...ObjectReceiveOption) *ObjectReceiver {
	r := &ObjectReceiver{
		db:              db,
		logger:          logger.WithName("ObjectReceiver"),
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

func (r *ObjectReceiver) logDuration(msg string) func(keysAndValues ...any) {
	start := time.Now()
	return func(keysAndValues ...any) {
		r.logger.WithValues(keysAndValues...).Info(msg, "duration", time.Since(start))
	}
}

func (r *ObjectReceiver) saveBlock(b []byte) (sum []byte, err error) {
	f := r.logDuration("save block")
	r.buf, err = s2.Decode(r.buf, b)
	if err != nil {
		return
	}
	if err = objects.ValidateBlockBytes(r.buf); err != nil {
		return
	}
	sum, err = objects.SaveCompressedBlock(r.db, r.buf, b)
	if err != nil {
		return
	}
	if r.saveObjHook != nil {
		r.saveObjHook(packfile.ObjectBlock, sum)
	}
	f("sum", hex.EncodeToString(sum))
	return
}

func (r *ObjectReceiver) saveTable(b []byte) (sum []byte, err error) {
	f := r.logDuration("save table")
	_, tbl, err := objects.ReadTableFrom(bytes.NewReader(b))
	if err != nil {
		return
	}
	sum, err = objects.SaveTable(r.db, b)
	if err != nil {
		return
	}
	if err = ingest.IndexTable(r.db, sum, tbl, r.logger.V(1)); err != nil {
		return
	}
	if err = ingest.ProfileTable(r.db, sum, tbl); err != nil {
		return
	}
	if r.saveObjHook != nil {
		r.saveObjHook(packfile.ObjectTable, sum)
	}
	f("sum", hex.EncodeToString(sum))
	return
}

func (r *ObjectReceiver) saveCommit(b []byte) (sum []byte, err error) {
	f := r.logDuration("save commit")
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
	f("sum", hex.EncodeToString(sum))
	return
}

func (r *ObjectReceiver) Receive(pr *packfile.PackfileReader, bar pbar.Bar) (done bool, err error) {
	for {
		ot, b, err := pr.ReadObject()
		if err != nil && err != io.EOF {
			return false, fmt.Errorf("read object error: %w", err)
		}
		var sum []byte
		switch ot {
		case packfile.ObjectBlock:
			sum, err = r.saveBlock(b)
			if err != nil {
				return false, fmt.Errorf("save block error: %w", err)
			}
		case packfile.ObjectTable:
			sum, err = r.saveTable(b)
			if err != nil {
				return false, fmt.Errorf("save table error: %w", err)
			}
		case packfile.ObjectCommit:
			sum, err = r.saveCommit(b)
			if err != nil {
				return false, fmt.Errorf("save commit error: %w", err)
			}
		default:
			if ot != 0 || len(b) != 0 {
				return false, fmt.Errorf("unrecognized object type %d", ot)
			}
		}
		if ot != 0 {
			if err = pr.Info.AddObject(ot, sum); err != nil {
				return false, fmt.Errorf("add object error: %w", err)
			}
		}
		if ot != 0 && bar != nil {
			bar.Incr()
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return len(r.expectedCommits) == 0, nil
}
