package versioning

import (
	"bytes"
	"encoding/gob"

	"github.com/wrgl/core/pkg/kv"
)

type Branch struct {
	CommitHash string
}

func branchKey(name string) []byte {
	return []byte("branch/" + name)
}

func (c *Branch) encode() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(c)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *Branch) Save(s kv.DB, name string) error {
	v, err := r.encode()
	if err != nil {
		return err
	}
	err = s.Set([]byte(branchKey(name)), v)
	if err != nil {
		return err
	}
	return nil
}

func decodeBranch(data []byte) (*Branch, error) {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	t := &Branch{}
	err := decoder.Decode(t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func GetBranch(s kv.DB, name string) (*Branch, error) {
	v, err := s.Get([]byte(branchKey(name)))
	if err != nil {
		return nil, err
	}
	var r *Branch
	r, err = decodeBranch(v)
	if err != nil {
		return nil, err
	}
	return r, nil
}
