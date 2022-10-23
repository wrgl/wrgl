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
	var idx *objects.BlockIndex
	for i, sum := range tbl.BlockIndices {
		if len(sum) == 0 {
			return &Issue{
				Err:        fmt.Sprintf("block index sum is empty at index: %d", i),
				Resolution: ReingestResolution,
				BlockIndex: sum,
			}
		}
		idx, bb, err = objects.GetBlockIndex(d.db, bb, sum)
		if err != nil {
			return &Issue{
				Err:        fmt.Sprintf("error getting block index: %v", err),
				Resolution: ReingestResolution,
				BlockIndex: sum,
			}
		}
		indicesRowsCount += idx.Len()
	}
	if indicesRowsCount != int(tbl.RowsCount) {
		return &Issue{
			Err:        fmt.Sprintf("index rows count does not match: %d vs %d", indicesRowsCount, tbl.RowsCount),
			Resolution: ReingestResolution,
		}
	}
	return nil
}

func (d *Doctor) addIssueInfo(iss *Issue, refname string, com *objects.Commit) {
	iss.Commit = com.Sum
	iss.Table = com.Table
	iss.Ref = refname
	if iss.Resolution == "" {
		iss.Resolution = UnknownResolution
	}
}

func (d *Doctor) diagnoseTree(tableIssues map[string]Issue, refname string, headSum []byte) (issues []*Issue, err error) {
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
				d.addIssueInfo(iss, refname, com)
			} else if prevCom != nil {
				iss.PreviousCommit = prevCom.Sum
			}
			issues = append(issues, iss)
			break
		}
		if com != nil {
			if v, ok := tableIssues[string(com.Table)]; ok {
				iss := &Issue{}
				*iss = v
				issues = append(issues, iss)
			} else if iss := d.diagnoseCommit(com); iss != nil {
				d.addIssueInfo(iss, refname, com)
				tableIssues[string(com.Table)] = *iss
				issues = append(issues, iss)
			}
		}
		if err == io.EOF {
			break
		}
		prevCom = com
	}
	for _, iss := range issues {
		ancestors, descendants, err := d.tree.Position(iss.Commit)
		if err != nil {
			return nil, err
		}
		iss.AncestorCount = ancestors
		iss.DescendantCount = descendants
		d.logger.Info("issue detected", "issue", iss)
	}
	return issues, nil
}
