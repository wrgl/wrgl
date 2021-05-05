package versioning

import (
	"github.com/wrgl/core/pkg/kv"
)

var (
	refPrefix  = []byte("refs/")
	headPrefix = append(refPrefix, []byte("heads/")...)
	tagPrefix  = append(refPrefix, []byte("tags/")...)
)

func headKey(name string) []byte {
	return append(headPrefix, []byte(name)...)
}

func tagKey(name string) []byte {
	return append(tagPrefix, []byte(name)...)
}

func SaveHead(s kv.DB, name string, commit []byte) error {
	return s.Set([]byte(headKey(name)), commit[:])
}

func SaveTag(s kv.DB, name string, commit []byte) error {
	return s.Set([]byte(tagKey(name)), commit[:])
}

func GetHead(s kv.DB, name string) ([]byte, error) {
	return s.Get([]byte(headKey(name)))
}

func GetTag(s kv.DB, name string) ([]byte, error) {
	return s.Get([]byte(tagKey(name)))
}

func listRefs(s kv.DB, prefix []byte) (map[string][]byte, error) {
	result := map[string][]byte{}
	m, err := s.Filter(prefix)
	if err != nil {
		return nil, err
	}
	l := len(prefix)
	for k, v := range m {
		name := k[l:]
		result[name] = v
	}
	return result, nil
}

func ListHeads(s kv.DB) (map[string][]byte, error) {
	return listRefs(s, headPrefix)
}

func ListTags(s kv.DB) (map[string][]byte, error) {
	return listRefs(s, tagPrefix)
}

func ListAllRefs(s kv.DB) (map[string][]byte, error) {
	return s.Filter(refPrefix)
}

func DeleteHead(s kv.DB, name string) error {
	return s.Delete(headKey(name))
}

func DeleteTag(s kv.DB, name string) error {
	return s.Delete(tagKey(name))
}
