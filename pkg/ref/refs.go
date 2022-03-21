// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package ref

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wrgl/wrgl/pkg/objects"
)

var (
	headPrefix           = "heads/"
	tagPrefix            = "tags/"
	remoteRefPrefix      = "remotes/"
	transactionRefPrefix = "transactions/"
)

func HeadRef(name string) string {
	return headPrefix + name
}

func tagRef(name string) string {
	return tagPrefix + name
}

func TransactionRef(tid, branch string) string {
	return fmt.Sprintf("%s%s/%s", transactionRefPrefix, tid, branch)
}

func RemoteRef(remote, name string) string {
	return fmt.Sprintf("%s%s/%s", string(remoteRefPrefix), remote, name)
}

func SaveFetchRef(s Store, name string, commit []byte, authorName, authorEmail, remote, message string) error {
	return SaveRef(s, name, commit, authorName, authorEmail, "fetch", fmt.Sprintf("[from %s] %s", remote, message), nil)
}

func SaveRef(s Store, name string, commit []byte, authorName, authorEmail, action, message string, txid *uuid.UUID) error {
	reflog := &Reflog{
		AuthorName:  authorName,
		AuthorEmail: authorEmail,
		Action:      action,
		Message:     message,
		Time:        time.Now(),
		NewOID:      commit,
		Txid:        txid,
	}
	if b, err := s.Get(name); err == nil {
		reflog.OldOID = b
	}
	return s.SetWithLog(name, commit, reflog)
}

func FirstLine(s string) string {
	i := bytes.IndexByte([]byte(s), '\n')
	if i == -1 {
		return s
	}
	return s[:i]
}

func CommitHead(s Store, name string, sum []byte, commit *objects.Commit, txid *uuid.UUID) error {
	return SaveRef(s, HeadRef(name), sum, commit.AuthorName, commit.AuthorEmail, "commit", FirstLine(commit.Message), txid)
}

func CommitMerge(s Store, branch string, sum []byte, commit *objects.Commit) error {
	parents := []string{}
	for _, parent := range commit.Parents {
		parents = append(parents, hex.EncodeToString(parent)[:7])
	}
	return SaveRef(s, HeadRef(branch), sum, commit.AuthorName, commit.AuthorEmail, "merge", fmt.Sprintf(
		"merge %s", strings.Join(parents, ", "),
	), nil)
}

func SaveTag(s Store, name string, sum []byte) error {
	return s.Set(tagRef(name), sum)
}

func SaveRemoteRef(s Store, remote, name string, commit []byte, authorName, authorEmail, action, message string) error {
	return SaveRef(s, RemoteRef(remote, name), commit, authorName, authorEmail, action, message, nil)
}

func SaveTransactionRef(s Store, tid uuid.UUID, branch string, sum []byte) error {
	return s.Set(TransactionRef(tid.String(), branch), sum)
}

func GetRef(s Store, name string) ([]byte, error) {
	return s.Get(name)
}

func GetHead(s Store, name string) ([]byte, error) {
	return s.Get(HeadRef(name))
}

func GetTag(s Store, name string) ([]byte, error) {
	return s.Get(tagRef(name))
}

func GetRemoteRef(s Store, remote, name string) ([]byte, error) {
	return s.Get(RemoteRef(remote, name))
}

func listRefs(s Store, prefix string) (map[string][]byte, error) {
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

func ListTransactionRefs(s Store, id uuid.UUID) (map[string][]byte, error) {
	return listRefs(s, TransactionRef(id.String(), ""))
}

func ListHeads(s Store) (map[string][]byte, error) {
	return listRefs(s, headPrefix)
}

func ListTags(s Store) (map[string][]byte, error) {
	return listRefs(s, tagPrefix)
}

func ListRemoteRefs(s Store, remote string) (map[string][]byte, error) {
	return listRefs(s, RemoteRef(remote, ""))
}

func ListAllRefs(s Store) (map[string][]byte, error) {
	return s.Filter("")
}

func ListLocalRefs(s Store) (map[string][]byte, error) {
	m, err := ListAllRefs(s)
	if err != nil {
		return nil, err
	}
	for k := range m {
		if strings.HasPrefix(k, remoteRefPrefix) {
			delete(m, k)
		}
	}
	return m, nil
}

func DeleteRef(s Store, name string) error {
	return s.Delete(name)
}

func DeleteHead(s Store, name string) error {
	return DeleteRef(s, "heads/"+name)
}

func DeleteTag(s Store, name string) error {
	return s.Delete(tagRef(name))
}

func DeleteRemoteRef(s Store, remote, name string) error {
	return DeleteRef(s, fmt.Sprintf("remotes/%s/%s", remote, name))
}

func DeleteAllRemoteRefs(s Store, remote string) error {
	keys, err := s.FilterKey(RemoteRef(remote, ""))
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

func DeleteTransactionRefs(s Store, id uuid.UUID) error {
	keys, err := s.FilterKey(TransactionRef(id.String(), ""))
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

func RenameRef(s Store, oldName, newName string) (sum []byte, err error) {
	sum, err = s.Get(oldName)
	if err != nil {
		return
	}
	err = s.Rename(oldName, newName)
	if err != nil {
		return
	}
	return sum, nil
}

func CopyRef(s Store, srcName, dstName string) (sum []byte, err error) {
	sum, err = s.Get(srcName)
	if err != nil {
		return
	}
	err = s.Copy(srcName, dstName)
	if err != nil {
		return
	}
	return sum, nil
}

func RenameAllRemoteRefs(s Store, oldRemote, newRemote string) error {
	prefix := RemoteRef(oldRemote, "")
	n := len(prefix)
	keys, err := s.FilterKey(prefix)
	if err != nil {
		return err
	}
	for _, k := range keys {
		name := string(k[n:])
		err = s.Rename(RemoteRef(oldRemote, name), RemoteRef(newRemote, name))
		if err != nil {
			return err
		}
	}
	return nil
}
