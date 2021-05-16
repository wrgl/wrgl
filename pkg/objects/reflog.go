package objects

import (
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/misc"
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

type ReflogWriter struct {
	w   io.Writer
	buf encoding.Bufferer
}

func NewReflogWriter(w io.Writer) *ReflogWriter {
	return &ReflogWriter{
		w:   w,
		buf: misc.NewBuffer(nil),
	}
}

func (w *ReflogWriter) Write(rec *Reflog) error {
	if rec.OldOID == nil {
		fmt.Fprintf(w.w, "%x ", make([]byte, 16))
	} else {
		fmt.Fprintf(w.w, "%x ", rec.OldOID)
	}
	fmt.Fprintf(w.w, "%x %s ", rec.NewOID, rec.AuthorName)
	if rec.AuthorEmail != "" {
		fmt.Fprintf(w.w, "<%s> ", rec.AuthorEmail)
	}
	fmt.Fprintf(w.w, "%s %s: %s\n", encoding.EncodeTime(rec.Time), rec.Action, rec.Message)
	return nil
}

type ReflogReader struct {
	scanner *misc.BackwardScanner
}

func NewReflogReader(r io.ReadSeeker) (*ReflogReader, error) {
	scanner, err := misc.NewBackwardScanner(r)
	if err != nil {
		return nil, err
	}
	return &ReflogReader{
		scanner: scanner,
	}, nil
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

func (r *ReflogReader) Read() (rec *Reflog, err error) {
	line := ""
	for line == "" {
		line, err = r.scanner.ReadLine()
		if err != nil {
			return
		}
	}
	rec = &Reflog{}
	state := rlStateOldOID
	off := 0
	n := len(line)
mainLoop:
	for {
		switch state {
		case rlStateOldOID:
			if !strings.HasPrefix(line, strings.Repeat("0", 32)) {
				rec.OldOID = make([]byte, 16)
				_, err = hex.Decode(rec.OldOID, []byte(line[:32]))
				if err != nil {
					return nil, err
				}
			}
			off += 33
			state = rlStateNewOID
		case rlStateNewOID:
			rec.NewOID = make([]byte, 16)
			_, err = hex.Decode(rec.NewOID, []byte(line[off:off+32]))
			if err != nil {
				return nil, err
			}
			off += 33
			state = rlStateName
		case rlStateName:
			for i := off + 1; i < n; i++ {
				c := line[i]
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
				return nil, fmt.Errorf("invalid reflog record: couldn't parse author name in record %q", line)
			}
		case rlStateEmail:
			for i := off + 1; i < n; i++ {
				c := line[i]
				if c == '>' {
					rec.AuthorEmail = line[off:i]
					state = rlStateTime
					off = i + 2
					break
				}
			}
			if rec.AuthorEmail == "" {
				return nil, fmt.Errorf("invalid reflog record: couldn't parse author email in record %q", line)
			}
		case rlStateTime:
			rec.Time, err = encoding.DecodeTime(line[off:])
			if err != nil {
				return nil, err
			}
			state = rlStateAction
			off += 17
		case rlStateAction:
			for i := off + 1; i < n; i++ {
				c := line[i]
				if c == ':' {
					rec.Action = line[off:i]
					state = rlStateMessage
					off = i + 2
					break
				}
			}
			if rec.Action == "" {
				return nil, fmt.Errorf("invalid reflog record: couldn't parse action in record %q", line)
			}
		case rlStateMessage:
			rec.Message = line[off:]
			break mainLoop
		}
	}
	return rec, nil
}
