package doctor

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/go-logr/logr"
	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/slice"
	"github.com/wrgl/wrgl/pkg/sorter"
)

// resolver resolves all issues in a single ref
type resolver struct {
	db            objects.Store
	tree          *Tree
	srt           *sorter.Sorter
	logger        logr.Logger
	resolvedTable map[string][]byte
}

func newResolver(db objects.Store, tree *Tree, logger logr.Logger) (*resolver, error) {
	srt, err := sorter.NewSorter()
	if err != nil {
		return nil, err
	}
	return &resolver{
		db:            db,
		tree:          tree,
		srt:           srt,
		logger:        logger.WithName("resolver"),
		resolvedTable: map[string][]byte{},
	}, nil
}

func (r *resolver) reset(iss *Issue, headCommit []byte) (err error) {
	if err = r.tree.Reset(headCommit); err != nil {
		return err
	}
	for {
		com, err := r.tree.Up()
		if err != nil && err != io.EOF {
			return err
		}
		if com != nil {
			if bytes.Equal(com.Sum, iss.Commit) {
				break
			}
		}
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("commit %x not found in ref %q: %v", iss.Commit, iss.Ref, io.ErrUnexpectedEOF)
		}
	}
	return nil
}

func (r *resolver) reingest(iss *Issue, resetPK bool) error {
	tblSum, ok := r.resolvedTable[string(iss.Table)]
	if !ok {
		tbl, err := objects.GetTable(r.db, iss.Table)
		if err != nil {
			return err
		}
		if resetPK {
			tbl.PK = nil
		}
		tblSum, err = r.ingestTable(iss, tbl)
		if err != nil {
			return err
		}
		r.resolvedTable[string(iss.Table)] = tblSum
	}
	return r.tree.EditCommit(iss.Commit, func(com *objects.Commit) (remove bool, update bool, err error) {
		com.Table = tblSum
		update = true
		return
	})
}

func (r *resolver) ingestTable(iss *Issue, tbl *objects.Table) (sum []byte, err error) {
	r.srt.Reset()
	r.srt.SetColumns(tbl.Columns)
	r.srt.PK, err = slice.KeyIndices(r.srt.Columns, tbl.PrimaryKey())
	if err != nil {
		return nil, err
	}
	bb := []byte{}
	var blk [][]string
	for _, blkSum := range tbl.Blocks {
		blk, bb, err = objects.GetBlock(r.db, bb, blkSum)
		if err != nil {
			return nil, fmt.Errorf("objects.GetBlock error: %v", err)
		}
		for _, row := range blk {
			r.srt.AddRow(row)
		}
	}
	inserter := ingest.NewInserter(r.db, r.srt, r.logger)
	return inserter.IngestTableFromSorter(r.srt.Columns, r.srt.PK)
}

func (r *resolver) remove(iss *Issue) error {
	return r.tree.EditCommit(iss.Commit, func(com *objects.Commit) (remove bool, update bool, err error) {
		remove = true
		return
	})
}

func (r *resolver) resolveIssue(iss *Issue) error {
	var err error
	switch iss.Resolution {
	case UnknownResolution:
		return fmt.Errorf("unknown resolution")
	case ResetPKResolution:
		err = r.reingest(iss, true)
	case ReingestResolution:
		err = r.reingest(iss, false)
	case RemoveResolution:
		err = r.remove(iss)
	default:
		return fmt.Errorf("unexpected resolution %q", iss.Resolution)
	}
	if err != nil {
		r.logger.Error(err, "error resolving issue", "issue", iss)
		return err
	}
	r.logger.Info("resolved issue", "issue", iss)
	return nil
}

func (r *resolver) updateRestOfTree() ([]byte, error) {
	return r.tree.UpdateAllDescendants()
}
