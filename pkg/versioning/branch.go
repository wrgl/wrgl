package versioning

import (
	"bytes"
	"encoding/gob"

	"github.com/wrgl/core/pkg/kv"
)

type Branch struct {
	CommitHash string
}

var branchPrefix = []byte("branch/")

func branchKey(name string) []byte {
	return append(branchPrefix, []byte(name)...)
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

func ListBranch(s kv.DB) (map[string]*Branch, error) {
	result := map[string]*Branch{}
	m, err := s.Filter(branchPrefix)
	if err != nil {
		return nil, err
	}
	for k, v := range m {
		name := k[len(branchPrefix):]
		b, err := decodeBranch(v)
		if err != nil {
			return nil, err
		}
		result[name] = b
	}
	return result, nil
}

func DeleteBranch(s kv.DB, name string) error {
	return s.Delete(branchKey(name))
}
