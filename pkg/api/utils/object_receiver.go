// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiutils

import (
	"bytes"
	"fmt"
	"io"

	"github.com/mmcloughlin/meow"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/ingest"
	"github.com/wrgl/core/pkg/objects"
)

type ObjectReceiver struct {
	db              objects.Store
	expectedCommits map[string]struct{}
	ReceivedCommits [][]byte
}

func NewObjectReceiver(db objects.Store, expectedCommits [][]byte) *ObjectReceiver {
	r := &ObjectReceiver{
		db:              db,
		expectedCommits: map[string]struct{}{},
	}
	for _, com := range expectedCommits {
		r.expectedCommits[string(com)] = struct{}{}
	}
	return r
}

func (r *ObjectReceiver) saveTable(b []byte) (err error) {
	_, tbl, err := objects.ReadTableFrom(bytes.NewReader(b))
	if err != nil {
		return
	}
	sum := meow.Checksum(0, b)
	err = ingest.IndexTable(r.db, sum[:], tbl)
	if err != nil {
		return
	}
	_, err = objects.SaveTable(r.db, b)
	return err
}

func (r *ObjectReceiver) saveCommit(b []byte) (err error) {
	_, com, err := objects.ReadCommitFrom(bytes.NewReader(b))
	if err != nil {
		return
	}
	if !objects.TableExist(r.db, com.Table) {
		return fmt.Errorf("table %x does not exist", com.Table)
	}
	for _, parent := range com.Parents {
		if !objects.CommitExist(r.db, parent) {
			return fmt.Errorf("parent commit %x does not exist", parent)
		}
	}
	sum, err := objects.SaveCommit(r.db, b)
	if err != nil {
		return
	}
	delete(r.expectedCommits, string(sum))
	r.ReceivedCommits = append(r.ReceivedCommits, sum)
	return nil
}

func (r *ObjectReceiver) Receive(pr *encoding.PackfileReader) (done bool, err error) {
	for {
		ot, b, err := pr.ReadObject()
		if err != nil && err != io.EOF {
			return false, err
		}
		switch ot {
		case encoding.ObjectBlock:
			_, err := objects.SaveBlock(r.db, b)
			if err != nil {
				return false, err
			}
		case encoding.ObjectTable:
			err := r.saveTable(b)
			if err != nil {
				return false, err
			}
		case encoding.ObjectCommit:
			err := r.saveCommit(b)
			if err != nil {
				return false, err
			}
		}
		if err == io.EOF {
			break
		}
	}
	return len(r.expectedCommits) == 0, nil
}
