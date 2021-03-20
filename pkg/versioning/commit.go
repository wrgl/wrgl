package versioning

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"time"

	"github.com/mmcloughlin/meow"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/table"
)

type Author struct {
	Email string
	Name  string
}

type Commit struct {
	Author         *Author
	Message        string
	ContentHash    string
	PrevCommitHash string
	Timestamp      time.Time
	TableStoreType table.StoreType
}

func (c *Commit) encode() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(c)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *Commit) GetTable(db kv.Store, fs kv.FileStore, seed uint64) (table.Store, error) {
	if c.TableStoreType == table.Big {
		return table.ReadBigStore(db, fs, seed, c.ContentHash)
	}
	return table.ReadSmallStore(db, seed, c.ContentHash)
}

func commitPrefix() []byte {
	return []byte("commit/")
}

func commitKey(hash string) []byte {
	return append(commitPrefix(), []byte(hash)...)
}

func (c *Commit) Save(s kv.DB, seed uint64) (string, error) {
	v, err := c.encode()
	if err != nil {
		return "", err
	}
	kb := meow.Checksum(seed, v)
	ks := hex.EncodeToString(kb[:])
	err = s.Set(commitKey(ks), v)
	if err != nil {
		return "", err
	}
	return ks, nil
}

func decodeCommit(data []byte) (*Commit, error) {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	t := &Commit{}
	err := decoder.Decode(t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func GetCommit(s kv.DB, hash string) (*Commit, error) {
	v, err := s.Get(commitKey(hash))
	if err != nil {
		return nil, err
	}
	var c *Commit
	c, err = decodeCommit(v)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func GetAllCommits(s kv.DB) ([]*Commit, error) {
	m, err := s.Filter(commitPrefix())
	if err != nil {
		return nil, err
	}
	result := []*Commit{}
	for _, v := range m {
		var c *Commit
		c, err = decodeCommit(v)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, nil
}

func GetAllCommitHashes(s kv.DB) ([]string, error) {
	prefix := commitPrefix()
	sl, err := s.FilterKey(prefix)
	if err != nil {
		return nil, err
	}
	l := len(prefix)
	result := []string{}
	for _, h := range sl {
		result = append(result, h[l:])
	}
	return result, nil
}

func DeleteCommit(s kv.DB, hash string) error {
	return s.Delete(commitKey(hash))
}

func DeleteAllCommit(s kv.Store) error {
	return s.Clear(commitPrefix())
}
