// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiclient

import (
	"bytes"
	"fmt"

	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

type RemoteFinder struct {
	travs map[string]*ref.Traveller
}

func NewRemoteFinder(db objects.Store, rs ref.Store) (*RemoteFinder, error) {
	m, err := ref.ListAllRefs(rs)
	if err != nil {
		return nil, err
	}
	travellers := map[string]*ref.Traveller{}
	for name := range m {
		travellers[name], err = ref.NewTraveller(db, rs, name)
		if err != nil {
			return nil, err
		}
	}
	return &RemoteFinder{
		travs: travellers,
	}, nil
}

func (f *RemoteFinder) FindRemoteFor(sum []byte) (string, error) {
	if sum == nil {
		return "", fmt.Errorf("empty commit sum")
	}
	for len(f.travs) > 0 {
		for name, t := range f.travs {
			com, err := t.Next()
			if err != nil {
				return "", err
			}
			if com == nil {
				t.Close()
				delete(f.travs, name)
			} else if bytes.Equal(com.Sum, sum) {
				if remote := t.Reflog.FetchRemote(); remote != "" {
					return remote, nil
				}
				t.Close()
				delete(f.travs, name)
			}
		}
	}
	return "", nil
}

func (f *RemoteFinder) Close() error {
	for _, t := range f.travs {
		if err := t.Close(); err != nil {
			return err
		}
	}
	return nil
}

func FindRemoteFor(db objects.Store, rs ref.Store, sum []byte) (string, error) {
	f, err := NewRemoteFinder(db, rs)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return f.FindRemoteFor(sum)
}
