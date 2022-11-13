// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ingest

import (
	"io"

	"github.com/go-logr/logr"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/sorter"
)

type ProgressBar interface {
	Add(int) error
	Finish() error
}

func IngestTableFromBlocks(db objects.Store, sorter *sorter.Sorter, columns []string, pk []uint32, blocks <-chan *sorter.Block, logger logr.Logger, opts ...InserterOption) ([]byte, error) {
	i := NewInserter(db, sorter, logger, opts...)
	i.blocks = blocks
	return i.ingestTableFromBlocks(columns, pk)
}

func IngestTable(db objects.Store, sorter *sorter.Sorter, f io.ReadCloser, pk []string, logger logr.Logger, opts ...InserterOption) ([]byte, error) {
	i := NewInserter(db, sorter, logger, opts...)
	return i.ingestTable(f, pk)
}
