package transaction

import (
	"bytes"
	"time"

	"github.com/google/uuid"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

func New(db objects.Store) (id uuid.UUID, err error) {
	id = uuid.New()
	tx := &objects.Transaction{
		Begin: time.Now(),
	}
	if err = objects.SaveTransaction(db, id, tx); err != nil {
		return
	}
	return id, nil
}

func Add(rs ref.Store, id uuid.UUID, branch string, comSum []byte) (err error) {
	return ref.SaveTransactionRef(rs, id, branch, comSum)
}

func Diff(rs ref.Store, id uuid.UUID) (map[string][2][]byte, error) {
	m, err := ref.ListTransactionRefs(rs, id)
	if err != nil {
		return nil, err
	}
	result := map[string][2][]byte{}
	for branch, sum := range m {
		result[branch] = [2][]byte{sum, nil}
		oldSum, err := ref.GetHead(rs, branch)
		if err == nil {
			result[branch] = [2][]byte{sum, oldSum}
		} else {
			result[branch] = [2][]byte{sum, nil}
		}
	}
	return result, nil
}

func Commit(db objects.Store, rs ref.Store, id uuid.UUID) (err error) {
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
		buf.Reset()
		_, err = com.WriteTo(buf)
		if err != nil {
			return err
		}
		newSum, err := objects.SaveCommit(db, buf.Bytes())
		if err != nil {
			return err
		}
		if err = ref.CommitHead(rs, branch, newSum, com); err != nil {
			return err
		}
	}
	return nil
}

func Discard(db objects.Store, rs ref.Store, id uuid.UUID) (err error) {
	if err = ref.DeleteTransactionRefs(rs, id); err != nil {
		return
	}
	return objects.DeleteTransaction(db, id)
}
