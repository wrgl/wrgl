package doctor

import (
	"bytes"
	"fmt"
	"io"

	"github.com/wrgl/wrgl/pkg/ingest"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
	"github.com/wrgl/wrgl/pkg/slice"
	"github.com/wrgl/wrgl/pkg/sorter"
)

// resolver resolves all issues in a single ref
type resolver struct {
	db       objects.Store
	tree     *Tree
	buf      *bytes.Buffer
	srt      *sorter.Sorter
	removals [][]byte
	m        commitMap
}

func newResolver(db objects.Store, tree *Tree) (*resolver, error) {
	srt, err := sorter.NewSorter()
	if err != nil {
		return nil, err
	}
	return &resolver{
		db:   db,
		tree: tree,
		buf:  bytes.NewBuffer(nil),
		srt:  srt,
	}, nil
}

func (r *resolver) reset(iss *Issue, headCommit []byte) (err error) {
	r.m = commitMap{}
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
		if err == io.EOF {
			return fmt.Errorf("commit %x not found in ref %q: %v", iss.Commit, iss.Ref, io.ErrUnexpectedEOF)
		}
	}
	return nil
}

func (r *resolver) reingest(iss *Issue, resetPK bool) error {
	tbl, err := objects.GetTable(r.db, iss.Table)
	if err != nil {
		return err
	}
	if resetPK {
		tbl.PK = nil
	}
	tblSum, err := r.ingestTable(iss, tbl)
	if err != nil {
		return err
	}
	return r.updateCommitUntil(iss, tblSum)
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
	inserter := ingest.NewInserter(r.db, r.srt)
	return inserter.IngestTableFromSorter(r.srt.Columns, r.srt.PK)
}

func (r *resolver) downTreeUntil(comSum []byte, each func(com *objects.Commit, equal bool) error) error {
	for {
		com, err := r.tree.Down()
		if err != nil && err != io.EOF {
			return err
		}
		if com != nil {
			equal := bytes.Equal(com.Sum, comSum)
			if err := each(com, equal); err != nil {
				return err
			}
			if equal {
				return nil
			}
		}
		if err == io.EOF {
			return fmt.Errorf("commit %x not found in tree: %v", comSum, io.ErrUnexpectedEOF)
		}
	}
}

func (r *resolver) updateCommit(com *objects.Commit) error {
	oldSum := com.Sum
	r.buf.Reset()
	if _, err := com.WriteTo(r.buf); err != nil {
		return err
	}
	sum, err := objects.SaveCommit(r.db, r.buf.Bytes())
	if err != nil {
		return err
	}
	r.m.update(oldSum, sum)
	return nil
}

func (r *resolver) updateCommitUntil(iss *Issue, newTableSum []byte) error {
	return r.downTreeUntil(iss.Commit, func(com *objects.Commit, equal bool) error {
		if equal {
			com.Table = newTableSum
		}
		if r.m.parentsUpdated(com) || equal {
			return r.updateCommit(com)
		}
		return nil
	})
}

func (r *resolver) remove(iss *Issue) error {
	r.downTreeUntil(iss.Commit, func(com *objects.Commit, equal bool) error {
		return nil
	})
	com, err := r.tree.Down()
	if err != nil && err != io.EOF {
		return err
	}
	if com != nil {
		// remove commit from its child's parents
		parents := make([][]byte, 0, len(com.Parents)-1)
		for _, b := range com.Parents {
			if !bytes.Equal(b, iss.Commit) {
				parents = append(parents, b)
			}
		}
		com.Parents = parents
		if err = r.updateCommit(com); err != nil {
			return err
		}
	}
	// remove commit and its ancestors
	q, err := ref.NewCommitsQueue(r.db, [][]byte{iss.Commit})
	if err != nil {
		return err
	}
	for {
		_, com, err := q.PopInsertParents()
		if err != nil && err != io.EOF {
			return err
		}
		if com != nil {
			r.removals = append(r.removals, com.Sum)
		}
		if err == io.EOF {
			return nil
		}
	}
}

func (r *resolver) resolveIssue(iss *Issue) error {
	switch iss.Resolution {
	case UnknownResolution:
		return fmt.Errorf("unknown resolution")
	case ResetPKResolution:
		return r.reingest(iss, true)
	case ReingestResolution:
		return r.reingest(iss, false)
	case RemoveResolution:
		return r.remove(iss)
	default:
		return fmt.Errorf("unexpected resolution %q", iss.Resolution)
	}
}
