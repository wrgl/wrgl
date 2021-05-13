package pack

import (
	"compress/gzip"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wrgl/core/pkg/encoding"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/table"
	"github.com/wrgl/core/pkg/versioning"
)

type UploadPackHandler struct {
	db          kv.DB
	fs          kv.FileStore
	negotiators map[string]*Negotiator
	Path        string
}

func NewUploadPackHandler(db kv.DB, fs kv.FileStore) *UploadPackHandler {
	return &UploadPackHandler{
		db:          db,
		fs:          fs,
		Path:        "/upload-pack/",
		negotiators: map[string]*Negotiator{},
	}
}

func (h *UploadPackHandler) getNegotiation(r *http.Request) (neg *Negotiator, nid string, err error) {
	var ok bool
	c, err := r.Cookie("negotiation-id")
	if err == nil {
		nid = c.Value
		neg, ok = h.negotiators[nid]
		if !ok {
			neg = nil
		}
	}
	if neg == nil {
		neg = NewNegotiator()
		var id uuid.UUID
		id, err = uuid.NewRandom()
		if err != nil {
			return
		}
		nid = id.String()
	}
	return
}

func (h *UploadPackHandler) sendACKs(rw http.ResponseWriter, nid string, neg *Negotiator, acks [][]byte) {
	rw.Header().Set("Content-Type", "application/x-wrgl-upload-pack-result")
	http.SetCookie(rw, &http.Cookie{
		Name:     "negotiation-id",
		Value:    nid,
		Path:     h.Path,
		HttpOnly: true,
		MaxAge:   3600 * 24,
	})
	h.negotiators[nid] = neg
	buf := misc.NewBuffer(nil)
	for _, ack := range acks {
		encoding.WritePktLine(rw, buf, "ACK "+hex.EncodeToString(ack))
	}
	encoding.WritePktLine(rw, buf, "NAK")
}

func (h *UploadPackHandler) getCommonTables(neg *Negotiator) map[string]struct{} {
	commonTables := map[string]struct{}{}
	for b := range neg.Commons {
		c, err := versioning.GetCommit(h.db, []byte(b))
		if err != nil {
			panic(err)
		}
		commonTables[string(c.Table)] = struct{}{}
	}
	return commonTables
}

func (h *UploadPackHandler) getTablesToSend(neg *Negotiator, buf *misc.Buffer, pw *encoding.PackfileWriter, commonTables map[string]struct{}) [][]byte {
	tables := [][]byte{}
	commits := neg.CommitsToSend()
	cw := objects.NewCommitWriter(buf)
	for _, c := range commits {
		buf.Reset()
		err := cw.Write(c)
		if err != nil {
			panic(err)
		}
		err = pw.WriteObject(encoding.ObjectCommit, buf.Bytes())
		if err != nil {
			panic(err)
		}
		if _, ok := commonTables[string(c.Table)]; ok {
			continue
		}
		tables = append(tables, c.Table)
	}
	return tables
}

func (h *UploadPackHandler) getCommonRows(commonTables map[string]struct{}) map[string]struct{} {
	commonRows := map[string]struct{}{}
	for b := range commonTables {
		t, err := table.ReadTable(h.db, h.fs, []byte(b))
		if err != nil {
			panic(err)
		}
		rhr := t.NewRowHashReader(0, 0)
		for {
			_, row, err := rhr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
			commonRows[string(row)] = struct{}{}
		}
	}
	return commonRows
}

func (h *UploadPackHandler) getRowsToSend(buf *misc.Buffer, pw *encoding.PackfileWriter, commonRows map[string]struct{}, tables [][]byte) [][]byte {
	tw := objects.NewTableWriter(buf)
	rows := [][]byte{}
	for _, b := range tables {
		buf.Reset()
		t, err := table.ReadTable(h.db, h.fs, []byte(b))
		if err != nil {
			panic(err)
		}
		err = tw.WriteMeta(t.Columns(), t.PrimaryKeyIndices())
		if err != nil {
			panic(err)
		}
		rhr := t.NewRowHashReader(0, 0)
		i := 0
		for {
			pk, row, err := rhr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
			err = tw.WriteRowAt(append(pk, row...), i)
			if err != nil {
				panic(err)
			}
			i++
			if _, ok := commonRows[string(row)]; ok {
				continue
			}
			rows = append(rows, row)
		}
		err = tw.Flush()
		if err != nil {
			panic(err)
		}
		err = pw.WriteObject(encoding.ObjectTable, buf.Bytes())
		if err != nil {
			panic(err)
		}
	}
	return rows
}

func (h *UploadPackHandler) sendRows(pw *encoding.PackfileWriter, rows [][]byte) {
	for _, row := range rows {
		obj, err := table.GetRow(h.db, row)
		if err != nil {
			panic(err)
		}
		err = pw.WriteObject(encoding.ObjectRow, obj)
		if err != nil {
			panic(err)
		}
	}
}

func (h *UploadPackHandler) sendPackfile(rw http.ResponseWriter, r *http.Request, neg *Negotiator) {
	rw.Header().Set("Content-Type", "application/x-wrgl-packfile")
	rw.Header().Set("Content-Encoding", "gzip")

	c, err := r.Cookie("negotiation-id")
	if err == nil {
		http.SetCookie(rw, &http.Cookie{
			Name:     "negotiation-id",
			Value:    c.Value,
			Path:     h.Path,
			HttpOnly: true,
			Expires:  time.Time{},
		})
		delete(h.negotiators, c.Value)
	}

	commonTables := h.getCommonTables(neg)
	commonRows := h.getCommonRows(commonTables)
	gzw := gzip.NewWriter(rw)
	defer gzw.Close()
	pw, err := encoding.NewPackfileWriter(gzw)
	if err != nil {
		panic(err)
	}
	buf := misc.NewBuffer(nil)
	tables := h.getTablesToSend(neg, buf, pw, commonTables)
	rows := h.getRowsToSend(buf, pw, commonRows, tables)
	h.sendRows(pw, rows)
}

func parseUploadPackRequest(r io.Reader) (wants, haves [][]byte, done bool, err error) {
	parser := encoding.NewParser(r)
	var s string
reading:
	for {
		s, err = encoding.ReadPktLine(parser)
		if err != nil {
			return
		}
		if s == "" {
			break
		}
		fields := strings.Fields(s)
		switch fields[0] {
		case "done":
			done = true
			break reading
		case "want":
			b := make([]byte, 16)
			_, err = hex.Decode(b, []byte(fields[1]))
			if err != nil {
				return
			}
			wants = append(wants, b)
		case "have":
			b := make([]byte, 16)
			_, err = hex.Decode(b, []byte(fields[1]))
			if err != nil {
				return
			}
			haves = append(haves, b)
		default:
			err = NewBadRequestError("unrecognized command %q", fields[0])
			return
		}
	}
	return
}

func (h *UploadPackHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "forbidden", http.StatusForbidden)
		return
	}
	wants, haves, done, err := parseUploadPackRequest(r.Body)
	if err != nil {
		panic(err)
	}
	neg, nid, err := h.getNegotiation(r)
	if err != nil {
		panic(err)
	}
	acks, err := neg.HandleUploadPackRequest(h.db, wants, haves, done)
	if err != nil {
		if v, ok := err.(*BadRequestError); ok {
			http.Error(rw, v.Message, http.StatusBadRequest)
			return
		}
		panic(err)
	}
	if len(acks) > 0 {
		h.sendACKs(rw, nid, neg, acks)
	} else {
		h.sendPackfile(rw, r, neg)
	}
}
