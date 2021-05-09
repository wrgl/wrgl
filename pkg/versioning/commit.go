package versioning

import (
	"bytes"

	"github.com/mmcloughlin/meow"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/slice"
)

var commitPrefix = []byte("commit/")

func commitKey(hash []byte) []byte {
	return append(commitPrefix, hash...)
}

func SaveCommit(s kv.DB, seed uint64, c *objects.Commit) ([]byte, error) {
	buf := bytes.NewBufferString("")
	writer := objects.NewCommitWriter(buf)
	err := writer.Write(c)
	if err != nil {
		return nil, err
	}
	v := buf.Bytes()
	kb := meow.Checksum(seed, v)
	sl := kb[:]
	err = s.Set(commitKey(sl), v)
	if err != nil {
		return nil, err
	}
	return sl, nil
}

func decodeCommit(data []byte) (*objects.Commit, error) {
	reader := objects.NewCommitReader(bytes.NewBuffer(data))
	return reader.Read()
}

func GetCommit(s kv.DB, hash []byte) (*objects.Commit, error) {
	v, err := s.Get(commitKey(hash))
	if err != nil {
		return nil, err
	}
	var c *objects.Commit
	c, err = decodeCommit(v)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func CommitExist(s kv.DB, hash []byte) bool {
	return s.Exist(commitKey(hash))
}

func GetAllCommits(s kv.DB) ([]*objects.Commit, error) {
	m, err := s.Filter(commitPrefix)
	if err != nil {
		return nil, err
	}
	result := []*objects.Commit{}
	for _, v := range m {
		var c *objects.Commit
		c, err = decodeCommit(v)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, nil
}

func GetAllCommitHashes(s kv.DB) ([][]byte, error) {
	sl, err := s.FilterKey(commitPrefix)
	if err != nil {
		return nil, err
	}
	l := len(commitPrefix)
	result := [][]byte{}
	for _, h := range sl {
		result = slice.InsertToSortedBytesSlice(result, []byte(h[l:]))
	}
	return result, nil
}

func DeleteCommit(s kv.DB, hash []byte) error {
	return s.Delete(commitKey(hash))
}

func DeleteAllCommit(s kv.Store) error {
	return s.Clear(commitPrefix)
}
