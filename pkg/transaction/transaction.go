// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package transaction

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/schollz/progressbar/v3"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

// Diff returns new sum and old sum for each branch in a transaction. If the
// transaction is still in progress, old sum for each branch is the current
// sum of that branch. If the transaction is already committed, then old sum
// for each branch is the oldoid recorded in reflog.
func Diff(rs ref.Store, id uuid.UUID) (map[string][2][]byte, *ref.Transaction, error) {
	tx, err := rs.GetTransaction(id)
	if err != nil {
		return nil, nil, ref.ErrKeyNotFound
	}
	result := map[string][2][]byte{}
	if tx.Status == ref.TSInProgress {
		m, err := ref.ListTransactionRefs(rs, id)
		if err != nil {
			return nil, nil, err
		}
		for branch, sum := range m {
			oldSum, err := ref.GetHead(rs, branch)
			if err == nil {
				result[branch] = [2][]byte{sum, oldSum}
			} else {
				result[branch] = [2][]byte{sum, nil}
			}
		}
	} else {
		logs, err := rs.GetTransactionLogs(id)
		if err != nil {
			return nil, nil, err
		}
		for branch, log := range logs {
			result[branch[6:]] = [2][]byte{log.NewOID, log.OldOID}
		}
	}
	return result, tx, nil
}

func Commit(db objects.Store, rs ref.Store, id uuid.UUID) (err error) {
	tx, err := rs.GetTransaction(id)
	if err != nil {
		return err
	}
	m, err := ref.ListTransactionRefs(rs, id)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(nil)
	for branch, sum := range m {
		com, err := objects.GetCommit(db, sum)
		if err != nil {
			return err
		}
		oldSum, err := ref.GetHead(rs, branch)
		if err == nil {
			com.Parents = [][]byte{oldSum}
		} else {
			com.Parents = nil
		}
		firstLine := ref.FirstLine(com.Message)
		com.Message = fmt.Sprintf("commit [tx/%s]\n%s", id, com.Message)
		buf.Reset()
		_, err = com.WriteTo(buf)
		if err != nil {
			return err
		}
		newSum, err := objects.SaveCommit(db, buf.Bytes())
		if err != nil {
			return err
		}
		if err = ref.SaveRef(rs, ref.HeadRef(branch), newSum, com.AuthorName, com.AuthorEmail, "commit", firstLine, &id); err != nil {
			return err
		}
	}
	tx.End = time.Now()
	tx.Status = ref.TSCommitted
	return rs.UpdateTransaction(tx)
}

func Discard(rs ref.Store, id uuid.UUID) (err error) {
	if err = ref.DeleteTransactionRefs(rs, id); err != nil {
		return
	}
	return rs.DeleteTransaction(id)
}

func GarbageCollect(db objects.Store, rs ref.Store, ttl time.Duration, pbar *progressbar.ProgressBar) (err error) {
	if pbar != nil {
		defer pbar.Finish()
	}
	ids, err := rs.GCTransactions(ttl)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err = ref.DeleteTransactionRefs(rs, id); err != nil {
			return
		}
	}
	return nil
}

func Reapply(db objects.Store, rs ref.Store, id uuid.UUID, cb func(branch string, sum []byte, message string)) (err error) {
	m, err := rs.GetTransactionLogs(id)
	if err != nil {
		return
	}
	buf := bytes.NewBuffer(nil)
	for name, rl := range m {
		name = strings.TrimPrefix(name, "heads/")
		oldSum, err := ref.GetHead(rs, name)
		if err != nil {
			return err
		}
		if bytes.Equal(oldSum, rl.NewOID) {
			cb(name, nil, "")
			continue
		}
		origCom, err := objects.GetCommit(db, rl.NewOID)
		if err != nil {
			return err
		}
		com := &objects.Commit{
			Time:        time.Now(),
			AuthorName:  origCom.AuthorName,
			AuthorEmail: origCom.AuthorEmail,
			Table:       origCom.Table,
			Message:     fmt.Sprintf("reapply [tx/%s]\n%s", id, origCom.Message),
			Parents:     [][]byte{oldSum},
		}
		buf.Reset()
		_, err = com.WriteTo(buf)
		if err != nil {
			return err
		}
		newSum, err := objects.SaveCommit(db, buf.Bytes())
		if err != nil {
			return err
		}
		if err = ref.SaveRef(rs, ref.HeadRef(name), newSum, com.AuthorName, com.AuthorEmail, "reapply", fmt.Sprintf("transaction %s", id), nil); err != nil {
			return err
		}
		cb(name, newSum, com.Message)
	}
	return nil
}
