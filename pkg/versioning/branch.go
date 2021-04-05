package versioning

import (
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
	"google.golang.org/protobuf/proto"
)

var branchPrefix = []byte("branch/")

func branchKey(name string) []byte {
	return append(branchPrefix, []byte(name)...)
}

func SaveBranch(s kv.DB, name string, branch *objects.Branch) error {
	v, err := proto.MarshalOptions{Deterministic: true}.Marshal(branch)
	if err != nil {
		return err
	}
	err = s.Set([]byte(branchKey(name)), v)
	if err != nil {
		return err
	}
	return nil
}

func decodeBranch(b []byte) (*objects.Branch, error) {
	obj := new(objects.Branch)
	err := proto.Unmarshal(b, obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func GetBranch(s kv.DB, name string) (*objects.Branch, error) {
	v, err := s.Get([]byte(branchKey(name)))
	if err != nil {
		return nil, err
	}
	return decodeBranch(v)
}

func ListBranch(s kv.DB) (map[string]*objects.Branch, error) {
	result := map[string]*objects.Branch{}
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
