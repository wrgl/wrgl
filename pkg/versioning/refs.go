package versioning

import (
	"fmt"
	"strings"

	"github.com/wrgl/core/pkg/kv"
)

var (
	refPrefix       = []byte("refs/")
	headPrefix      = append(refPrefix, []byte("heads/")...)
	tagPrefix       = append(refPrefix, []byte("tags/")...)
	remoteRefPrefix = append(refPrefix, []byte("remotes/")...)
)

func refKey(name string) []byte {
	return append(refPrefix, []byte(name)...)
}

func headKey(name string) []byte {
	return append(headPrefix, []byte(name)...)
}

func tagKey(name string) []byte {
	return append(tagPrefix, []byte(name)...)
}

func remoteRefKey(remote, name string) []byte {
	return []byte(fmt.Sprintf("%s%s/%s", string(remoteRefPrefix), remote, name))
}

func SaveHead(s kv.DB, name string, commit []byte) error {
	return s.Set(headKey(name), commit)
}

func SaveTag(s kv.DB, name string, commit []byte) error {
	return s.Set(tagKey(name), commit)
}

func SaveRemoteRef(s kv.DB, remote, name string, commit []byte) error {
	return s.Set(remoteRefKey(remote, name), commit)
}

func SaveRef(s kv.DB, name string, commit []byte) error {
	return s.Set(refKey(name), commit)
}

func GetRef(s kv.DB, name string) ([]byte, error) {
	return s.Get(refKey(name))
}

func GetHead(s kv.DB, name string) ([]byte, error) {
	return s.Get(headKey(name))
}

func GetTag(s kv.DB, name string) ([]byte, error) {
	return s.Get(tagKey(name))
}

func GetRemoteRef(s kv.DB, remote, name string) ([]byte, error) {
	return s.Get(remoteRefKey(remote, name))
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

func ListRemoteRefs(s kv.DB, remote string) (map[string][]byte, error) {
	return listRefs(s, remoteRefKey(remote, ""))
}

func ListAllRefs(s kv.DB) (map[string][]byte, error) {
	return s.Filter(refPrefix)
}

func ListLocalRefs(s kv.DB) (map[string][]byte, error) {
	m, err := ListAllRefs(s)
	if err != nil {
		return nil, err
	}
	for k := range m {
		if strings.HasPrefix(k, string(remoteRefPrefix)) {
			delete(m, k)
		}
	}
	return m, nil
}

func DeleteHead(s kv.DB, name string) error {
	return s.Delete(headKey(name))
}

func DeleteTag(s kv.DB, name string) error {
	return s.Delete(tagKey(name))
}

func DeleteRemoteRef(s kv.DB, remote, name string) error {
	return s.Delete(remoteRefKey(remote, name))
}

func DeleteAllRemoteRefs(s kv.DB, remote string) error {
	keys, err := s.FilterKey(remoteRefKey(remote, ""))
	if err != nil {
		return err
	}
	for _, b := range keys {
		err = s.Delete(b)
		if err != nil {
			return err
		}
	}
	return nil
}

func RenameAllRemoteRefs(s kv.DB, oldRemote, newRemote string) error {
	prefix := remoteRefKey(oldRemote, "")
	n := len(prefix)
	m, err := s.Filter(prefix)
	if err != nil {
		return err
	}
	for k, v := range m {
		err = s.Set(remoteRefKey(newRemote, k[n:]), v)
		if err != nil {
			return err
		}
		err = s.Delete([]byte(k))
		if err != nil {
			return err
		}
	}
	return nil
}
