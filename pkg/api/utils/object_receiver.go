// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiutils

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/compress/s2"
	"github.com/pckhoi/meow"
	"github.com/wrgl/wrgl/pkg/encoding"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/objects"
)

type ObjectReceiver struct {
	db              objects.Store
	expectedCommits map[string]struct{}
	ReceivedCommits [][]byte
	debugOut        io.Writer
	buf             []byte
}

func NewObjectReceiver(db objects.Store, expectedCommits [][]byte, debugOut io.Writer) *ObjectReceiver {
	r := &ObjectReceiver{
		db:              db,
		expectedCommits: map[string]struct{}{},
		debugOut:        debugOut,
	}
	for _, com := range expectedCommits {
		r.expectedCommits[string(com)] = struct{}{}
	}
	return r
}

func (r *ObjectReceiver) saveBlock(b []byte) (err error) {
	r.buf, err = s2.Decode(r.buf, b)
	if err != nil {
		return
	}
	if err = objects.ValidateBlockBytes(r.buf); err != nil {
		return
	}
	_, err = objects.SaveCompressedBlock(r.db, r.buf, b)
	return err
}

func (r *ObjectReceiver) saveTable(b []byte) (err error) {
	_, tbl, err := objects.ReadTableFrom(bytes.NewReader(b))
	if err != nil {
		return
	}
	sum := meow.Checksum(0, b)
	if err = ingest.IndexTable(r.db, sum[:], tbl, r.debugOut); err != nil {
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
			err := r.saveBlock(b)
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
		default:
			if ot != 0 || len(b) != 0 {
				return false, fmt.Errorf("unrecognized object type %d", ot)
			}
		}
		if err == io.EOF {
			break
		}
	}
	return len(r.expectedCommits) == 0, nil
}
