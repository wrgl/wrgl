// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package ref

import (
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/wrgl/wrgl/pkg/encoding"
)

type Reflog struct {
	OldOID      []byte
	NewOID      []byte
	AuthorName  string
	AuthorEmail string
	Time        time.Time
	Action      string
	Message     string
}

func (rec *Reflog) WriteTo(w io.Writer) (total int64, err error) {
	var n int
	if rec.OldOID == nil {
		n, err = w.Write([]byte(fmt.Sprintf("%x ", make([]byte, 16))))
	} else {
		n, err = w.Write([]byte(fmt.Sprintf("%x ", rec.OldOID)))
	}
	if err != nil {
		return 0, err
	}
	total += int64(n)
	n, err = w.Write([]byte(fmt.Sprintf("%x %s ", rec.NewOID, rec.AuthorName)))
	if err != nil {
		return 0, err
	}
	total += int64(n)
	if rec.AuthorEmail != "" {
		n, err = w.Write([]byte(fmt.Sprintf("<%s> ", rec.AuthorEmail)))
		if err != nil {
			return 0, err
		}
		total += int64(n)
	}
	n, err = w.Write([]byte(fmt.Sprintf("%s %s: %s", encoding.EncodeTime(rec.Time), rec.Action, rec.Message)))
	if err != nil {
		return 0, err
	}
	total += int64(n)
	return total, nil
}

const (
	rlStateOldOID int = iota
	rlStateNewOID
	rlStateName
	rlStateEmail
	rlStateTime
	rlStateAction
	rlStateMessage
)

func (rec *Reflog) Read(b []byte) (n int, err error) {
	state := rlStateOldOID
	off := 0
	n = len(b)
	line := string(b)
mainLoop:
	for {
		switch state {
		case rlStateOldOID:
			if !strings.HasPrefix(line, strings.Repeat("0", 32)) {
				rec.OldOID = make([]byte, 16)
				_, err = hex.Decode(rec.OldOID, []byte(b[:32]))
				if err != nil {
					return 0, err
				}
			}
			off += 33
			state = rlStateNewOID
		case rlStateNewOID:
			rec.NewOID = make([]byte, 16)
			_, err = hex.Decode(rec.NewOID, []byte(b[off:off+32]))
			if err != nil {
				return 0, err
			}
			off += 33
			state = rlStateName
		case rlStateName:
			for i := off + 1; i < n; i++ {
				c := b[i]
				if c == '<' {
					rec.AuthorName = line[off : i-1]
					state = rlStateEmail
					off = i + 1
					break
				} else if c >= 48 && c <= 57 {
					// c is a numeric rune
					rec.AuthorName = line[off : i-1]
					state = rlStateTime
					off = i
					break
				}
			}
			if rec.AuthorName == "" {
				return 0, fmt.Errorf("invalid reflog record: couldn't parse author name in record %q", line)
			}
		case rlStateEmail:
			for i := off + 1; i < n; i++ {
				c := b[i]
				if c == '>' {
					rec.AuthorEmail = line[off:i]
					state = rlStateTime
					off = i + 2
					break
				}
			}
			if rec.AuthorEmail == "" {
				return 0, fmt.Errorf("invalid reflog record: couldn't parse author email in record %q", line)
			}
		case rlStateTime:
			rec.Time, err = encoding.DecodeTime(line[off:])
			if err != nil {
				return 0, err
			}
			state = rlStateAction
			off += 17
		case rlStateAction:
			for i := off + 1; i < n; i++ {
				c := b[i]
				if c == ':' {
					rec.Action = line[off:i]
					state = rlStateMessage
					off = i + 2
					break
				}
			}
			if rec.Action == "" {
				return 0, fmt.Errorf("invalid reflog record: couldn't parse action in record %q", line)
			}
		case rlStateMessage:
			rec.Message = line[off:]
			break mainLoop
		}
	}
	return n, nil
}
