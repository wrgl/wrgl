// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package reffs

import (
	"io"

	"github.com/wrgl/wrgl/pkg/misc"
	"github.com/wrgl/wrgl/pkg/ref"
)

type logReader struct {
	scanner *misc.BackwardScanner
	r       io.Closer
}

func NewReflogReader(r io.ReadSeekCloser) (ref.ReflogReader, error) {
	scanner, err := misc.NewBackwardScanner(r)
	if err != nil {
		return nil, err
	}
	return &logReader{
		scanner: scanner,
		r:       r,
	}, nil
}

func (r *logReader) Read() (rec *ref.Reflog, err error) {
	line := ""
	for line == "" {
		line, err = r.scanner.ReadLine()
		if err != nil {
			return
		}
	}
	rec = &ref.Reflog{}
	_, err = rec.Read([]byte(line))
	return rec, err
}

func (r *logReader) Close() error {
	return r.r.Close()
}
