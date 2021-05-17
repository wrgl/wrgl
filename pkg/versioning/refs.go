package versioning

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/objects"
)

var (
	refPrefix       = []byte("refs/")
	headPrefix      = "heads/"
	tagPrefix       = "tags/"
	remoteRefPrefix = "remotes/"
)

func refKey(name string) []byte {
	return append(refPrefix, []byte(name)...)
}

func headRef(name string) string {
	return headPrefix + name
}

func tagRef(name string) string {
	return tagPrefix + name
}

func remoteRef(remote, name string) string {
	return fmt.Sprintf("%s%s/%s", string(remoteRefPrefix), remote, name)
}

func reflogKey(refKey []byte) []byte {
	return append([]byte("logs/"), refKey...)
}

func SaveRef(s kv.DB, fs kv.FileStore, name string, commit []byte, authorName, authorEmail, action, message string) error {
	reflog := &objects.Reflog{
		AuthorName:  authorName,
		AuthorEmail: authorEmail,
		Action:      action,
		Message:     message,
		Time:        time.Now(),
		NewOID:      commit,
	}
	key := refKey(name)
	if b, err := s.Get(key); err == nil {
		reflog.OldOID = b
	}
	w, err := fs.AppendWriter(reflogKey(key))
	if err != nil {
		return err
	}
	defer w.Close()
	writer := objects.NewReflogWriter(w)
	err = writer.Write(reflog)
	if err != nil {
		return err
	}
	return s.Set(key, commit)
}

func CommitHead(s kv.DB, fs kv.FileStore, name string, sum []byte, commit *objects.Commit) error {
	i := bytes.IndexByte([]byte(commit.Message), '\n')
	var message string
	if i == -1 {
		message = commit.Message
	} else {
		message = commit.Message[:i]
	}
	return SaveRef(s, fs, headRef(name), sum, commit.AuthorName, commit.AuthorEmail, "commit", message)
}

func SaveTag(s kv.DB, name string, sum []byte) error {
	return s.Set(refKey(tagRef(name)), sum)
}

func SaveRemoteRef(s kv.DB, fs kv.FileStore, remote, name string, commit []byte, authorName, authorEmail, action, message string) error {
	return SaveRef(s, fs, remoteRef(remote, name), commit, authorName, authorEmail, action, message)
}

func GetRef(s kv.DB, name string) ([]byte, error) {
	return s.Get(refKey(name))
}

func GetHead(s kv.DB, name string) ([]byte, error) {
	return s.Get(refKey(headRef(name)))
}

func GetTag(s kv.DB, name string) ([]byte, error) {
	return s.Get(refKey(tagRef(name)))
}

func GetRemoteRef(s kv.DB, remote, name string) ([]byte, error) {
	return s.Get(refKey(remoteRef(remote, name)))
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
	return listRefs(s, refKey(headPrefix))
}

func ListTags(s kv.DB) (map[string][]byte, error) {
	return listRefs(s, refKey(tagPrefix))
}

func ListRemoteRefs(s kv.DB, remote string) (map[string][]byte, error) {
	return listRefs(s, refKey(remoteRef(remote, "")))
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
		if strings.HasPrefix(k, string(append(refPrefix, remoteRefPrefix...))) {
			delete(m, k)
		}
	}
	return m, nil
}

func deleteRef(s kv.DB, fs kv.FileStore, name string) error {
	err := fs.Delete(reflogKey(refKey(name)))
	if err != nil {
		return err
	}
	return s.Delete(refKey(name))
}

func DeleteHead(s kv.DB, fs kv.FileStore, name string) error {
	return deleteRef(s, fs, "heads/"+name)
}

func DeleteTag(s kv.DB, name string) error {
	return s.Delete(refKey(tagRef(name)))
}

func DeleteRemoteRef(s kv.DB, fs kv.FileStore, remote, name string) error {
	return deleteRef(s, fs, fmt.Sprintf("remotes/%s/%s", remote, name))
}

func DeleteAllRemoteRefs(s kv.DB, fs kv.FileStore, remote string) error {
	keys, err := s.FilterKey(refKey(remoteRef(remote, "")))
	if err != nil {
		return err
	}
	for _, b := range keys {
		err = fs.Delete(reflogKey(b))
		if err != nil {
			return err
		}
		err = s.Delete(b)
		if err != nil {
			return err
		}
	}
	return nil
}

func RenameRef(s kv.DB, fs kv.FileStore, oldName, newName string) error {
	oldKey := refKey(oldName)
	newKey := refKey(newName)
	v, err := s.Get(oldKey)
	if err != nil {
		return err
	}
	err = s.Set(newKey, v)
	if err != nil {
		return err
	}
	err = s.Delete(oldKey)
	if err != nil {
		return err
	}
	return fs.Move(reflogKey(oldKey), reflogKey(newKey))
}

func RenameAllRemoteRefs(s kv.DB, fs kv.FileStore, oldRemote, newRemote string) error {
	prefix := refKey(remoteRef(oldRemote, ""))
	n := len(prefix)
	keys, err := s.FilterKey(prefix)
	if err != nil {
		return err
	}
	for _, k := range keys {
		name := string(k[n:])
		err = RenameRef(s, fs, string(remoteRef(oldRemote, name)), string(remoteRef(newRemote, name)))
		if err != nil {
			return err
		}
	}
	return nil
}
