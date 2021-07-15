// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiclient

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	apiutils "github.com/wrgl/core/pkg/api/utils"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/ref"
)

type ReceivePackSession struct {
	sender  *apiutils.ObjectSender
	c       *Client
	updates []*apiutils.Update
}

func NewReceivePackSession(db objects.Store, rs ref.Store, c *Client, updates []*apiutils.Update, remoteRefs map[string][]byte, maxPackfileSize uint64) (*ReceivePackSession, error) {
	wants := [][]byte{}
	for _, u := range updates {
		if u.Sum != nil {
			wants = append(wants, u.Sum)
		}
	}
	haves := make([][]byte, 0, len(remoteRefs))
	for _, sum := range remoteRefs {
		haves = append(haves, sum)
	}
	finder := apiutils.NewClosedSetsFinder(db, rs)
	_, err := finder.Process(wants, haves, true)
	if err != nil {
		return nil, err
	}
	sender, err := apiutils.NewObjectSender(db, finder.CommitsToSend(), finder.CommonCommmits(), maxPackfileSize)
	if err != nil {
		return nil, err
	}
	return &ReceivePackSession{
		sender:  sender,
		updates: updates,
		c:       c,
	}, nil
}

func parseReceivePackResult(r io.ReadCloser) (status map[string]string, err error) {
	defer r.Close()
	parser := encoding.NewParser(r)
	line, err := encoding.ReadPktLine(parser)
	if err != nil {
		return
	}
	if !strings.HasPrefix(line, "unpack ") {
		err = fmt.Errorf("unrecognized payload: %q", line)
		return
	}
	if line[7:] != "ok" {
		err = fmt.Errorf("unpack error: %s", line[7:])
		return
	}
	status = map[string]string{}
	for {
		line, err = encoding.ReadPktLine(parser)
		if err == io.EOF {
			break
		}
		if err != nil {
			return
		}
		if line == "" {
			break
		}
		if strings.HasPrefix(line, "ok ") {
			status[line[3:]] = ""
		} else if strings.HasPrefix(line, "ng ") {
			i := bytes.IndexByte([]byte(line[3:]), ' ') + 3
			status[line[3:i]] = line[i+1:]
		} else {
			err = fmt.Errorf("unrecognized payload: %q", line)
			return
		}
	}
	return
}

func (s *ReceivePackSession) Start() (updates []*apiutils.Update, err error) {
	var body io.ReadCloser
	for {
		done := false
		body, err = s.c.PostReceivePack(s.updates, func(w io.Writer) error {
			done, err = s.sender.WriteObjects(w)
			return err
		})
		if err != nil {
			return
		}
		if done {
			break
		}
	}
	status, err := parseReceivePackResult(body)
	if err != nil {
		return
	}
	for _, u := range s.updates {
		if s, ok := status[u.Dst]; ok {
			u.ErrMsg = s
		} else {
			u.ErrMsg = "remote failed to report status"
		}
	}
	return s.updates, nil
}
