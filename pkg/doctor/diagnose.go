package doctor

import (
	"fmt"
	"io"

	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/slice"
)

func (d *Doctor) diagnoseCommit(com *objects.Commit) *Issue {
	tbl, err := objects.GetTable(d.db, com.Table)
	if err != nil {
		return &Issue{
			Err:        fmt.Sprintf("error fetching table: %v", err.Error()),
			Resolution: RemoveResolution,
		}
	}
	n := len(tbl.Columns)
	for i, k := range tbl.PK {
		if int(k) >= n {
			return &Issue{
				Err:        fmt.Sprintf("pk index greater than columns count: [%d]: %d", i, k),
				Resolution: ResetPKResolution,
			}
		}
	}
	for i, s := range tbl.PrimaryKey() {
		if s == "" {
			return &Issue{
				Err:        fmt.Sprintf("primary key column is empty: %d", i),
				Resolution: ResetPKResolution,
			}
		}
	}
	rowsCount := 0
	var bb []byte
	var blk [][]string
	var prevRow = make([]string, len(tbl.Columns))
	for i, sum := range tbl.Blocks {
		if len(sum) == 0 {
			return &Issue{
				Err:        fmt.Sprintf("block sum is empty at index: %d", i),
				Resolution: RemoveResolution,
				Block:      sum,
			}
		}
		blk, bb, err = objects.GetBlock(d.db, bb, sum)
		if err != nil {
			return &Issue{
				Err:        fmt.Sprintf("error getting block: %v", err),
				Resolution: RemoveResolution,
				Block:      sum,
			}
		}
		for j := 0; j < len(blk); j++ {
			if slice.StringSliceEqual(blk[j], prevRow) {
				return &Issue{
					Err:        fmt.Sprintf("duplicated rows: %d,%d", j-1, j),
					Resolution: ReingestResolution,
					Block:      sum,
				}
			}
			copy(prevRow, blk[j])
		}
		rowsCount += len(blk)
	}
	if rowsCount != int(tbl.RowsCount) {
		return &Issue{
			Err:        fmt.Sprintf("rows count does not match: %d vs %d", rowsCount, tbl.RowsCount),
			Resolution: ReingestResolution,
		}
	}
	if len(tbl.BlockIndices) != len(tbl.Blocks) {
		return &Issue{
			Err:        fmt.Sprintf("block indices count does not match: %d vs %d", len(tbl.BlockIndices), len(tbl.Blocks)),
			Resolution: ReingestResolution,
		}
	}
	indicesRowsCount := 0
	for i, sum := range tbl.BlockIndices {
		if len(sum) == 0 {
			return &Issue{
				Err:        fmt.Sprintf("block index sum is empty at index: %d", i),
				Resolution: ReingestResolution,
				BlockIndex: sum,
			}
		}
		blk, bb, err = objects.GetBlock(d.db, bb, sum)
		if err != nil {
			return &Issue{
				Err:        fmt.Sprintf("error getting block index: %v", err),
				Resolution: ReingestResolution,
				BlockIndex: sum,
			}
		}
		indicesRowsCount += len(blk)
	}
	if indicesRowsCount != int(tbl.RowsCount) {
		return &Issue{
			Err:        fmt.Sprintf("index rows count does not match: %d vs %d", indicesRowsCount, tbl.RowsCount),
			Resolution: ReingestResolution,
		}
	}
	return nil
}

func (d *Doctor) addIssueInfo(iss *Issue, refname string, com *objects.Commit) error {
	ancestors, descendants, err := d.tree.Position(com.Sum)
	if err != nil {
		return err
	}
	iss.AncestorCount = ancestors
	iss.DescendantCount = descendants
	iss.Commit = com.Sum
	iss.Table = com.Table
	iss.Ref = refname
	if iss.Resolution == "" {
		iss.Resolution = UnknownResolution
	}
	return nil
}

func (d *Doctor) diagnoseTree(refname string, headSum []byte) (issues []*Issue, err error) {
	if err = d.tree.Reset(headSum); err != nil {
		return
	}
	var com *objects.Commit
	var prevCom *objects.Commit
	for {
		com, err = d.tree.Up()
		if err != nil && err != io.EOF {
			iss := &Issue{
				Err:        fmt.Sprintf("error popping commits: %v", err),
				Resolution: UnknownResolution,
				Ref:        refname,
			}
			if com != nil {
				if err := d.addIssueInfo(iss, refname, com); err != nil {
					return nil, err
				}
			} else if prevCom != nil {
				iss.PreviousCommit = prevCom.Sum
			}
			issues = append(issues, iss)
			break
		}
		if com != nil {
			if iss := d.diagnoseCommit(com); iss != nil {
				if err := d.addIssueInfo(iss, refname, com); err != nil {
					return nil, err
				}
				issues = append(issues, iss)
			}
		}
		if err == io.EOF {
			break
		}
		prevCom = com
	}
	return issues, nil
}
